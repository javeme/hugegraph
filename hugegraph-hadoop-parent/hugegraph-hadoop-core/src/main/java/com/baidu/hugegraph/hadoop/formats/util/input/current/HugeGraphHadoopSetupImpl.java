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

package com.baidu.hugegraph.hadoop.formats.util.input.current;

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.core.HugeGraphFactory;
import com.baidu.hugegraph.core.HugeGraphVertex;
import com.baidu.hugegraph.diskstorage.configuration.BasicConfiguration;
import com.baidu.hugegraph.graphdb.database.RelationReader;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;
import com.baidu.hugegraph.graphdb.idmanagement.IDManager;
import com.baidu.hugegraph.graphdb.internal.HugeGraphSchemaCategory;
import com.baidu.hugegraph.graphdb.query.QueryUtil;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import com.baidu.hugegraph.graphdb.types.TypeDefinitionCategory;
import com.baidu.hugegraph.graphdb.types.TypeDefinitionMap;
import com.baidu.hugegraph.graphdb.types.TypeInspector;
import com.baidu.hugegraph.graphdb.types.system.BaseKey;
import com.baidu.hugegraph.graphdb.types.system.BaseLabel;
import com.baidu.hugegraph.graphdb.types.vertices.HugeGraphSchemaVertex;
import com.baidu.hugegraph.hadoop.config.ModifiableHadoopConfiguration;
import com.baidu.hugegraph.hadoop.config.HugeGraphHadoopConfiguration;
import com.baidu.hugegraph.hadoop.formats.util.input.SystemTypeInspector;
import com.baidu.hugegraph.hadoop.formats.util.input.HugeGraphHadoopSetupCommon;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HugeGraphHadoopSetupImpl extends HugeGraphHadoopSetupCommon {

    private final ModifiableHadoopConfiguration scanConf;
    private final StandardHugeGraph graph;
    private final StandardHugeGraphTx tx;

    public HugeGraphHadoopSetupImpl(final Configuration config) {
        scanConf = ModifiableHadoopConfiguration.of(HugeGraphHadoopConfiguration.MAPRED_NS, config);
        BasicConfiguration bc = scanConf.getHugeGraphConf();
        graph = (StandardHugeGraph) HugeGraphFactory.open(bc);
        tx = (StandardHugeGraphTx) graph.buildTransaction().readOnly().vertexCacheSize(200).start();
    }

    @Override
    public TypeInspector getTypeInspector() {
        // Pre-load schema
        for (HugeGraphSchemaCategory sc : HugeGraphSchemaCategory.values()) {
            for (HugeGraphVertex k : QueryUtil.getVertices(tx, BaseKey.SchemaCategory, sc)) {
                assert k instanceof HugeGraphSchemaVertex;
                HugeGraphSchemaVertex s = (HugeGraphSchemaVertex) k;
                if (sc.hasName()) {
                    String name = s.name();
                    Preconditions.checkNotNull(name);
                }
                TypeDefinitionMap dm = s.getDefinition();
                Preconditions.checkNotNull(dm);
                s.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.OUT);
                s.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.IN);
            }
        }
        return tx;
    }

    @Override
    public SystemTypeInspector getSystemTypeInspector() {
        return new SystemTypeInspector() {
            @Override
            public boolean isSystemType(long typeid) {
                return IDManager.isSystemRelationTypeId(typeid);
            }

            @Override
            public boolean isVertexExistsSystemType(long typeid) {
                return typeid == BaseKey.VertexExists.longId();
            }

            @Override
            public boolean isVertexLabelSystemType(long typeid) {
                return typeid == BaseLabel.VertexLabelEdge.longId();
            }

            @Override
            public boolean isTypeSystemType(long typeid) {
                return typeid == BaseKey.SchemaCategory.longId() || typeid == BaseKey.SchemaDefinitionProperty.longId()
                        || typeid == BaseKey.SchemaDefinitionDesc.longId() || typeid == BaseKey.SchemaName.longId()
                        || typeid == BaseLabel.SchemaDefinitionEdge.longId();
            }
        };
    }

    @Override
    public IDManager getIDManager() {
        return graph.getIDManager();
    }

    @Override
    public RelationReader getRelationReader(long vertexid) {
        return graph.getEdgeSerializer();
    }

    @Override
    public void close() {
        tx.rollback();
        graph.close();
    }

    @Override
    public boolean getFilterPartitionedVertices() {
        return scanConf.get(HugeGraphHadoopConfiguration.FILTER_PARTITIONED_VERTICES);
    }
}
