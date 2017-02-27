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

package com.baidu.hugegraph.graphdb.olap.computer;

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.core.HugeGraphVertex;
import com.baidu.hugegraph.diskstorage.EntryList;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;
import com.baidu.hugegraph.graphdb.idmanagement.IDManager;
import com.baidu.hugegraph.graphdb.olap.VertexJobConverter;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import com.baidu.hugegraph.graphdb.util.WorkerPool;
import com.baidu.hugegraph.graphdb.vertices.PreloadedVertex;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PartitionedVertexProgramExecutor<M> {

    private static final Logger log =
            LoggerFactory.getLogger(PartitionedVertexProgramExecutor.class);

    private final StandardHugeGraph graph;
    private final IDManager idManager;
    private final FulgoraMemory memory;
    private final FulgoraVertexMemory<M> vertexMemory;
    private final VertexProgram<M> vertexProgram;


    public static final String GHOTST_PARTITION_VERTEX = "partition-ghost";
    public static final String PARTITION_VERTEX_POSTSUCCESS = "partition-success";
    public static final String PARTITION_VERTEX_POSTFAIL = "partition-fail";

    public PartitionedVertexProgramExecutor(StandardHugeGraph graph, FulgoraMemory memory,
                                 FulgoraVertexMemory vertexMemory, VertexProgram<M> vertexProgram) {
        this.graph=graph;
        this.idManager = graph.getIDManager();
        this.memory = memory;
        this.vertexMemory = vertexMemory;
        this.vertexProgram = vertexProgram;
    }

    public void run(int numThreads, ScanMetrics metrics) {
        StandardHugeGraphTx tx=null;
        Map<Long,EntryList> pVertexAggregates = vertexMemory.retrievePartitionAggregates();
        if (pVertexAggregates.isEmpty()) return; //Nothing to do here

        try (WorkerPool workers = new WorkerPool(numThreads)) {
            tx = VertexJobConverter.startTransaction(graph);
            for (Map.Entry<Long,EntryList> pvertices : pVertexAggregates.entrySet()) {
                if (pvertices.getValue()==null) {
                    metrics.incrementCustom(GHOTST_PARTITION_VERTEX);
                    continue;
                }
                workers.submit(new PartitionedVertexProcessor(pvertices.getKey(),pvertices.getValue(),tx,metrics));
            }
        } catch (Throwable ex) {
            log.error("Could not post-process partitioned vertices", ex);
            metrics.incrementCustom(PARTITION_VERTEX_POSTFAIL);
        } finally {
            if (tx!=null && tx.isOpen()) tx.rollback();
        }
    }

    private class PartitionedVertexProcessor implements Runnable {

        private final long vertexId;
        private final EntryList preloaded;
        private final StandardHugeGraphTx tx;
        private final ScanMetrics metrics;

        private PartitionedVertexProcessor(long vertexId, EntryList preloaded, StandardHugeGraphTx tx, ScanMetrics metrics) {
            Preconditions.checkArgument(idManager.isPartitionedVertex(vertexId) && idManager.isCanonicalVertexId(vertexId));
            assert preloaded!=null;
            this.vertexId = vertexId;
            this.preloaded = preloaded;
            this.tx = tx;
            this.metrics = metrics;
        }

        @Override
        public void run() {
            try {
                HugeGraphVertex vertex = tx.getInternalVertex(vertexId);
                Preconditions.checkArgument(vertex instanceof PreloadedVertex,
                        "The bounding transaction is not configured correctly");
                PreloadedVertex v = (PreloadedVertex)vertex;
                v.setAccessCheck(PreloadedVertex.OPENSTAR_CHECK);
                v.addToQueryCache(VertexProgramScanJob.SYSTEM_PROPS_QUERY,preloaded);
                VertexMemoryHandler.Partition<M> vh = new VertexMemoryHandler.Partition<M>(vertexMemory,v);
                v.setPropertyMixing(vh);
                vertexProgram.execute(v,vh,memory);
                metrics.incrementCustom(PARTITION_VERTEX_POSTSUCCESS);
            } catch (Throwable e) {
                metrics.incrementCustom(PARTITION_VERTEX_POSTFAIL);
                log.error("Error post-processing partition vertex: " + vertexId,e);
            }
        }
    }

}
