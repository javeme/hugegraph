// Copyright 2017 HugeGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.baidu.hugegraph.graphdb.olap.job;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.baidu.hugegraph.core.*;
import com.baidu.hugegraph.core.schema.*;
import com.baidu.hugegraph.diskstorage.BackendTransaction;
import com.baidu.hugegraph.diskstorage.Entry;
import com.baidu.hugegraph.diskstorage.StaticBuffer;
import com.baidu.hugegraph.diskstorage.configuration.Configuration;
import com.baidu.hugegraph.diskstorage.indexing.IndexEntry;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.baidu.hugegraph.graphdb.database.EdgeSerializer;
import com.baidu.hugegraph.graphdb.database.IndexSerializer;
import com.baidu.hugegraph.graphdb.database.management.RelationTypeIndexWrapper;
import com.baidu.hugegraph.graphdb.internal.InternalRelation;
import com.baidu.hugegraph.graphdb.internal.InternalRelationType;
import com.baidu.hugegraph.graphdb.olap.QueryContainer;
import com.baidu.hugegraph.graphdb.olap.VertexScanJob;
import com.baidu.hugegraph.graphdb.relations.EdgeDirection;
import com.baidu.hugegraph.graphdb.types.CompositeIndexType;
import com.baidu.hugegraph.graphdb.types.IndexType;
import com.baidu.hugegraph.graphdb.types.MixedIndexType;
import com.baidu.hugegraph.graphdb.types.system.BaseLabel;
import com.baidu.hugegraph.graphdb.types.vertices.HugeGraphSchemaVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexRepairJob extends IndexUpdateJob implements VertexScanJob {

    /**
     * The number of composite-index entries modified or added to the storage backend by this job.
     */
    public static final String ADDED_RECORDS_COUNT = "adds";

    /**
     * The number of mixed-index documents (or whatever idiom is equivalent to the document in the backend
     * implementation) modified by this job
     */
    public static final String DOCUMENT_UPDATES_COUNT = "doc-updates";

    public IndexRepairJob() {
        super();
    }

    protected IndexRepairJob(IndexRepairJob job) {
        super(job);
    }

    public IndexRepairJob(final String indexName, final String indexType) {
        super(indexName, indexType);
    }

    @Override
    public void workerIterationEnd(ScanMetrics metrics) {
        super.workerIterationEnd(metrics);
    }

    @Override
    public void workerIterationStart(final HugeGraph graph, Configuration config, ScanMetrics metrics) {
        super.workerIterationStart(graph, config, metrics);
    }

    /**
     * Check that our target index is in either the ENABLED or REGISTERED state.
     */
    @Override
    protected void validateIndexStatus() {
        HugeGraphSchemaVertex schemaVertex = mgmt.getSchemaVertex(index);
        Set<SchemaStatus> acceptableStatuses = SchemaAction.REINDEX.getApplicableStatus();
        boolean isValidIndex = true;
        String invalidIndexHint;
        if (index instanceof RelationTypeIndex
                || (index instanceof HugeGraphIndex && ((HugeGraphIndex) index).isCompositeIndex())) {
            SchemaStatus actualStatus = schemaVertex.getStatus();
            isValidIndex = acceptableStatuses.contains(actualStatus);
            invalidIndexHint = String.format("The index has status %s, but one of %s is required", actualStatus,
                    acceptableStatuses);
        } else {
            Preconditions.checkArgument(index instanceof HugeGraphIndex, "Unexpected index: %s", index);
            HugeGraphIndex gindex = (HugeGraphIndex) index;
            Preconditions.checkArgument(gindex.isMixedIndex());
            Map<String, SchemaStatus> invalidKeyStatuses = new HashMap<>();
            int acceptableFields = 0;
            for (PropertyKey key : gindex.getFieldKeys()) {
                SchemaStatus status = gindex.getIndexStatus(key);
                if (status != SchemaStatus.DISABLED && !acceptableStatuses.contains(status)) {
                    isValidIndex = false;
                    invalidKeyStatuses.put(key.name(), status);
                    log.warn("Index {} has key {} in an invalid status {}", index, key, status);
                }
                if (acceptableStatuses.contains(status))
                    acceptableFields++;
            }
            invalidIndexHint = String.format(
                    "The following index keys have invalid status: %s (status must be one of %s)",
                    Joiner.on(",").withKeyValueSeparator(" has status ").join(invalidKeyStatuses), acceptableStatuses);
            if (isValidIndex && acceptableFields == 0) {
                isValidIndex = false;
                invalidIndexHint = "The index does not contain any valid keys";
            }
        }
        Preconditions.checkArgument(isValidIndex, "The index %s is in an invalid state and cannot be indexed. %s",
                indexName, invalidIndexHint);
        // TODO consider retrieving the current Job object and calling killJob() if !isValidIndex -- would be more
        // efficient than throwing an exception on the first pair processed by each mapper
        log.debug("Index {} is valid for re-indexing");
    }

    @Override
    public void process(HugeGraphVertex vertex, ScanMetrics metrics) {
        try {
            BackendTransaction mutator = writeTx.getTxHandle();
            if (index instanceof RelationTypeIndex) {
                RelationTypeIndexWrapper wrapper = (RelationTypeIndexWrapper) index;
                InternalRelationType wrappedType = wrapper.getWrappedType();
                EdgeSerializer edgeSerializer = writeTx.getEdgeSerializer();
                List<Entry> additions = new ArrayList<>();

                for (Object relation : vertex.query().types(indexRelationTypeName).direction(Direction.OUT)
                        .relations()) {
                    InternalRelation hugegraphRelation = (InternalRelation) relation;
                    for (int pos = 0; pos < hugegraphRelation.getArity(); pos++) {
                        if (!wrappedType.isUnidirected(Direction.BOTH)
                                && !wrappedType.isUnidirected(EdgeDirection.fromPosition(pos)))
                            continue; // Directionality is not covered
                        Entry entry = edgeSerializer.writeRelation(hugegraphRelation, wrappedType, pos, writeTx);
                        additions.add(entry);
                    }
                }
                StaticBuffer vertexKey = writeTx.getIdInspector().getKey(vertex.longId());
                mutator.mutateEdges(vertexKey, additions, KCVSCache.NO_DELETIONS);
                metrics.incrementCustom(ADDED_RECORDS_COUNT, additions.size());
            } else if (index instanceof HugeGraphIndex) {
                IndexType indexType = mgmt.getSchemaVertex(index).asIndexType();
                assert indexType != null;
                IndexSerializer indexSerializer = graph.getIndexSerializer();
                // Gather elements to index
                List<HugeGraphElement> elements;
                switch (indexType.getElement()) {
                    case VERTEX:
                        elements = ImmutableList.of(vertex);
                        break;
                    case PROPERTY:
                        elements = Lists.newArrayList();
                        for (HugeGraphVertexProperty p : addIndexSchemaConstraint(vertex.query(), indexType)
                                .properties()) {
                            elements.add(p);
                        }
                        break;
                    case EDGE:
                        elements = Lists.newArrayList();
                        for (Object e : addIndexSchemaConstraint(vertex.query().direction(Direction.OUT), indexType)
                                .edges()) {
                            elements.add((HugeGraphEdge) e);
                        }
                        break;
                    default:
                        throw new AssertionError("Unexpected category: " + indexType.getElement());
                }
                if (indexType.isCompositeIndex()) {
                    for (HugeGraphElement element : elements) {
                        Set<IndexSerializer.IndexUpdate<StaticBuffer, Entry>> updates =
                                indexSerializer.reindexElement(element, (CompositeIndexType) indexType);
                        for (IndexSerializer.IndexUpdate<StaticBuffer, Entry> update : updates) {
                            log.debug("Mutating index {}: {}", indexType, update.getEntry());
                            mutator.mutateIndex(update.getKey(), Lists.newArrayList(update.getEntry()),
                                    KCVSCache.NO_DELETIONS);
                            metrics.incrementCustom(ADDED_RECORDS_COUNT);
                        }
                    }
                } else {
                    assert indexType.isMixedIndex();
                    Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new HashMap<>();
                    for (HugeGraphElement element : elements) {
                        indexSerializer.reindexElement(element, (MixedIndexType) indexType, documentsPerStore);
                        metrics.incrementCustom(DOCUMENT_UPDATES_COUNT);
                    }
                    mutator.getIndexTransaction(indexType.getBackingIndexName()).restore(documentsPerStore);
                }

            } else
                throw new UnsupportedOperationException("Unsupported index found: " + index);
        } catch (final Exception e) {
            mgmt.rollback();
            writeTx.rollback();
            metrics.incrementCustom(FAILED_TX);
            throw new HugeGraphException(e.getMessage(), e);
        }
    }

    @Override
    public void getQueries(QueryContainer queries) {
        if (index instanceof RelationTypeIndex) {
            queries.addQuery().types(indexRelationTypeName).direction(Direction.OUT).relations();
        } else if (index instanceof HugeGraphIndex) {
            IndexType indexType = mgmt.getSchemaVertex(index).asIndexType();
            switch (indexType.getElement()) {
                case PROPERTY:
                    addIndexSchemaConstraint(queries.addQuery(), indexType).properties();
                    break;
                case VERTEX:
                    queries.addQuery().properties();
                    queries.addQuery().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).edges();
                    break;
                case EDGE:
                    indexType.hasSchemaTypeConstraint();
                    addIndexSchemaConstraint(queries.addQuery().direction(Direction.OUT), indexType).edges();
                    break;
                default:
                    throw new AssertionError("Unexpected category: " + indexType.getElement());
            }
        } else
            throw new UnsupportedOperationException("Unsupported index found: " + index);
    }

    @Override
    public IndexRepairJob clone() {
        return new IndexRepairJob(this);
    }

    private static <Q extends BaseVertexQuery> Q addIndexSchemaConstraint(Q query, IndexType indexType) {
        if (indexType.hasSchemaTypeConstraint()) {
            HugeGraphSchemaType constraint = indexType.getSchemaTypeConstraint();
            Preconditions.checkArgument(constraint instanceof RelationType,
                    "Expected constraint to be a " + "relation type: %s", constraint);
            query.types((RelationType) constraint);
        }
        return query;
    }
}
