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

package com.baidu.hugegraph.hadoop;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.baidu.hugegraph.core.HugeGraph;
import com.baidu.hugegraph.core.schema.RelationTypeIndex;
import com.baidu.hugegraph.core.schema.SchemaAction;
import com.baidu.hugegraph.core.schema.HugeGraphIndex;
import com.baidu.hugegraph.core.schema.Index;
import com.baidu.hugegraph.core.schema.HugeGraphManagement;
import com.baidu.hugegraph.diskstorage.Backend;
import com.baidu.hugegraph.diskstorage.BackendException;
import com.baidu.hugegraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.baidu.hugegraph.diskstorage.cassandra.astyanax.AstyanaxStoreManager;
import com.baidu.hugegraph.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager;
import com.baidu.hugegraph.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.baidu.hugegraph.diskstorage.configuration.ConfigElement;
import com.baidu.hugegraph.diskstorage.hbase.HBaseStoreManager;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;
import com.baidu.hugegraph.graphdb.olap.job.IndexRemoveJob;
import com.baidu.hugegraph.graphdb.olap.job.IndexRepairJob;
import com.baidu.hugegraph.graphdb.olap.job.IndexUpdateJob;
import com.baidu.hugegraph.hadoop.config.ModifiableHadoopConfiguration;
import com.baidu.hugegraph.hadoop.config.HugeGraphHadoopConfiguration;
import com.baidu.hugegraph.hadoop.formats.cassandra.CassandraBinaryInputFormat;
import com.baidu.hugegraph.hadoop.formats.hbase.HBaseBinaryInputFormat;
import com.baidu.hugegraph.hadoop.scan.HadoopScanMapper;
import com.baidu.hugegraph.hadoop.scan.HadoopScanRunner;
import com.baidu.hugegraph.hadoop.scan.HadoopVertexScanMapper;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration.ROOT_NS;

public class MapReduceIndexManagement {

    private static final Logger log = LoggerFactory.getLogger(MapReduceIndexManagement.class);

    private final StandardHugeGraph graph;

    private static final EnumSet<SchemaAction> SUPPORTED_ACTIONS =
            EnumSet.of(SchemaAction.REINDEX, SchemaAction.REMOVE_INDEX);

    private static final String SUPPORTED_ACTIONS_STRING = Joiner.on(", ").join(SUPPORTED_ACTIONS);

    private static final Set<Class<? extends KeyColumnValueStoreManager>> CASSANDRA_STORE_MANAGER_CLASSES = ImmutableSet
            .of(CassandraEmbeddedStoreManager.class, AstyanaxStoreManager.class, CassandraThriftStoreManager.class);

    private static final Set<Class<? extends KeyColumnValueStoreManager>> HBASE_STORE_MANAGER_CLASSES =
            ImmutableSet.of(HBaseStoreManager.class);

    public MapReduceIndexManagement(HugeGraph g) {
        this.graph = (StandardHugeGraph) g;
    }

    /**
     * Updates the provided index according to the given {@link SchemaAction}. Only {@link SchemaAction#REINDEX} and
     * {@link SchemaAction#REMOVE_INDEX} are supported.
     *
     * @param index the index to process
     * @param updateAction either {@code REINDEX} or {@code REMOVE_INDEX}
     * @return a future that returns immediately; this method blocks until the Hadoop MapReduce job completes
     */
    // TODO make this future actually async and update javadoc @return accordingly
    public HugeGraphManagement.IndexJobFuture updateIndex(Index index, SchemaAction updateAction)
            throws BackendException {

        Preconditions.checkNotNull(index, "Index parameter must not be null", index);
        Preconditions.checkNotNull(updateAction, "%s parameter must not be null", SchemaAction.class.getSimpleName());
        Preconditions.checkArgument(SUPPORTED_ACTIONS.contains(updateAction),
                "Only these %s parameters are supported: %s (was given %s)", SchemaAction.class.getSimpleName(),
                SUPPORTED_ACTIONS_STRING, updateAction);
        Preconditions.checkArgument(
                RelationTypeIndex.class.isAssignableFrom(index.getClass())
                        || HugeGraphIndex.class.isAssignableFrom(index.getClass()),
                "Index %s has class %s: must be a %s or %s (or subtype)", index.getClass(),
                RelationTypeIndex.class.getSimpleName(), HugeGraphIndex.class.getSimpleName());

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        ModifiableHadoopConfiguration hugegraphmrConf =
                ModifiableHadoopConfiguration.of(HugeGraphHadoopConfiguration.MAPRED_NS, hadoopConf);

        // The job we'll execute to either REINDEX or REMOVE_INDEX
        final Class<? extends IndexUpdateJob> indexJobClass;
        final Class<? extends Mapper> mapperClass;

        // The class of the IndexUpdateJob and the Mapper that will be used to run it (VertexScanJob vs ScanJob)
        if (updateAction.equals(SchemaAction.REINDEX)) {
            indexJobClass = IndexRepairJob.class;
            mapperClass = HadoopVertexScanMapper.class;
        } else if (updateAction.equals(SchemaAction.REMOVE_INDEX)) {
            indexJobClass = IndexRemoveJob.class;
            mapperClass = HadoopScanMapper.class;
        } else {
            // Shouldn't get here -- if this exception is ever thrown, update SUPPORTED_ACTIONS
            throw new IllegalStateException("Unrecognized " + SchemaAction.class.getSimpleName() + ": " + updateAction);
        }

        // The column family that serves as input to the IndexUpdateJob
        final String readCF;
        if (RelationTypeIndex.class.isAssignableFrom(index.getClass())) {
            readCF = Backend.EDGESTORE_NAME;
        } else {
            HugeGraphIndex gindex = (HugeGraphIndex) index;
            if (gindex.isMixedIndex() && !updateAction.equals(SchemaAction.REINDEX))
                throw new UnsupportedOperationException(
                        "External mixed indexes must be removed in the indexing system directly.");

            Preconditions.checkState(HugeGraphIndex.class.isAssignableFrom(index.getClass()));
            if (updateAction.equals(SchemaAction.REMOVE_INDEX))
                readCF = Backend.INDEXSTORE_NAME;
            else
                readCF = Backend.EDGESTORE_NAME;
        }
        hugegraphmrConf.set(HugeGraphHadoopConfiguration.COLUMN_FAMILY_NAME, readCF);

        // The MapReduce InputFormat class based on the open graph's store manager
        final Class<? extends InputFormat> inputFormat;
        final Class<? extends KeyColumnValueStoreManager> storeManagerClass = graph.getBackend().getStoreManagerClass();
        if (CASSANDRA_STORE_MANAGER_CLASSES.contains(storeManagerClass)) {
            inputFormat = CassandraBinaryInputFormat.class;
            // Set the partitioner
            IPartitioner part =
                    ((AbstractCassandraStoreManager) graph.getBackend().getStoreManager()).getCassandraPartitioner();
            hadoopConf.set("cassandra.input.partitioner.class", part.getClass().getName());
        } else if (HBASE_STORE_MANAGER_CLASSES.contains(storeManagerClass)) {
            inputFormat = HBaseBinaryInputFormat.class;
        } else {
            throw new IllegalArgumentException("Store manager class " + storeManagerClass + "is not supported");
        }

        // The index name and relation type name (if the latter is applicable)
        final String indexName = index.name();
        final String relationTypeName = RelationTypeIndex.class.isAssignableFrom(index.getClass())
                ? ((RelationTypeIndex) index).getType().name() : "";
        Preconditions.checkNotNull(indexName);

        // Set the class of the IndexUpdateJob
        hugegraphmrConf.set(HugeGraphHadoopConfiguration.SCAN_JOB_CLASS, indexJobClass.getName());
        // Set the configuration of the IndexUpdateJob
        copyIndexJobKeys(hadoopConf, indexName, relationTypeName);
        hugegraphmrConf.set(HugeGraphHadoopConfiguration.SCAN_JOB_CONFIG_ROOT,
                GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        // Copy the StandardHugeGraph configuration under HugeGraphHadoopConfiguration.GRAPH_CONFIG_KEYS
        org.apache.commons.configuration.Configuration localbc = graph.getConfiguration().getLocalConfiguration();
        localbc.clearProperty(Graph.GRAPH);
        copyInputKeys(hadoopConf, localbc);

        String jobName = HadoopScanMapper.class.getSimpleName() + "[" + indexJobClass.getSimpleName() + "]";

        try {
            return new CompletedJobFuture(HadoopScanRunner.runJob(hadoopConf, inputFormat, jobName, mapperClass));
        } catch (Exception e) {
            return new FailedJobFuture(e);
        }
    }

    private static void copyInputKeys(org.apache.hadoop.conf.Configuration hadoopConf,
            org.apache.commons.configuration.Configuration source) {
        // Copy IndexUpdateJob settings into the hadoop-backed cfg
        Iterator<String> keyIter = source.getKeys();
        while (keyIter.hasNext()) {
            String key = keyIter.next();
            ConfigElement.PathIdentifier pid;
            try {
                pid = ConfigElement.parse(ROOT_NS, key);
            } catch (RuntimeException e) {
                log.debug("[inputkeys] Skipping {}", key, e);
                continue;
            }

            if (!pid.element.isOption())
                continue;

            String k = ConfigElement.getPath(HugeGraphHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + "." + key;
            String v = source.getProperty(key).toString();

            hadoopConf.set(k, v);
            log.debug("[inputkeys] Set {}={}", k, v);
        }
    }

    private static void copyIndexJobKeys(org.apache.hadoop.conf.Configuration hadoopConf, String indexName,
            String relationType) {
        hadoopConf.set(ConfigElement.getPath(HugeGraphHadoopConfiguration.SCAN_JOB_CONFIG_KEYS, true) + "."
                + ConfigElement.getPath(IndexUpdateJob.INDEX_NAME), indexName);

        hadoopConf.set(ConfigElement.getPath(HugeGraphHadoopConfiguration.SCAN_JOB_CONFIG_KEYS, true) + "."
                + ConfigElement.getPath(IndexUpdateJob.INDEX_RELATION_TYPE), relationType);

        hadoopConf.set(
                ConfigElement.getPath(HugeGraphHadoopConfiguration.SCAN_JOB_CONFIG_KEYS, true) + "."
                        + ConfigElement.getPath(GraphDatabaseConfiguration.JOB_START_TIME),
                String.valueOf(System.currentTimeMillis()));
    }

    private static class CompletedJobFuture implements HugeGraphManagement.IndexJobFuture {

        private final ScanMetrics completedJobMetrics;

        private CompletedJobFuture(ScanMetrics completedJobMetrics) {
            this.completedJobMetrics = completedJobMetrics;
        }

        @Override
        public ScanMetrics getIntermediateResult() {
            return completedJobMetrics;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public ScanMetrics get() throws InterruptedException, ExecutionException {
            return completedJobMetrics;
        }

        @Override
        public ScanMetrics get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return completedJobMetrics;
        }
    }

    private static class FailedJobFuture implements HugeGraphManagement.IndexJobFuture {

        private final Throwable cause;

        public FailedJobFuture(Throwable cause) {
            this.cause = cause;
        }

        @Override
        public ScanMetrics getIntermediateResult() throws ExecutionException {
            throw new ExecutionException(cause);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public ScanMetrics get() throws InterruptedException, ExecutionException {
            throw new ExecutionException(cause);
        }

        @Override
        public ScanMetrics get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            throw new ExecutionException(cause);
        }
    }
}
