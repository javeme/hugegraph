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

package com.baidu.hugegraph.graphdb.tinkerpop;

import com.baidu.hugegraph.diskstorage.keycolumnvalue.StoreFeatures;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * Blueprint's features of a HugeGraph.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class HugeGraphFeatures implements Graph.Features {

    private final GraphFeatures graphFeatures;
    private final VertexFeatures vertexFeatures;
    private final EdgeFeatures edgeFeatures;

    private final StandardHugeGraph graph;

    private HugeGraphFeatures(StandardHugeGraph graph, StoreFeatures storageFeatures) {
        graphFeatures = new HugeGraphGeneralFeatures(storageFeatures.supportsPersistence());
        vertexFeatures = new HugeGraphVertexFeatures();
        edgeFeatures = new HugeGraphEdgeFeatures();
        this.graph = graph;
    }

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public static HugeGraphFeatures getFeatures(StandardHugeGraph graph, StoreFeatures storageFeatures) {
        return new HugeGraphFeatures(graph, storageFeatures);
    }

    private static class HugeGraphDataTypeFeatures implements DataTypeFeatures {

        @Override
        public boolean supportsMapValues() {
            return true;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }

    private static class HugeGraphVariableFeatures extends HugeGraphDataTypeFeatures implements VariableFeatures {
    }

    private static class HugeGraphGeneralFeatures extends HugeGraphDataTypeFeatures implements GraphFeatures {

        private final boolean persists;

        private HugeGraphGeneralFeatures(boolean persists) {
            this.persists = persists;
        }

        @Override
        public VariableFeatures variables() {
            return new HugeGraphVariableFeatures();
        }

        @Override
        public boolean supportsComputer() {
            return true;
        }

        @Override
        public boolean supportsPersistence() {
            return persists;
        }

        @Override
        public boolean supportsTransactions() {
            return true;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return true;
        }
    }

    private static class HugeGraphVertexPropertyFeatures extends HugeGraphDataTypeFeatures
            implements VertexPropertyFeatures {

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }
    }

    private static class HugeGraphEdgePropertyFeatures extends HugeGraphDataTypeFeatures
            implements EdgePropertyFeatures {

    }

    private class HugeGraphVertexFeatures implements VertexFeatures {

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            StandardHugeGraphTx tx = (StandardHugeGraphTx) HugeGraphFeatures.this.graph.newTransaction();
            try {
                if (!tx.containsPropertyKey(key))
                    return tx.getConfiguration().getAutoSchemaMaker().defaultPropertyCardinality(key).convert();
                return tx.getPropertyKey(key).cardinality().convert();
            } finally {
                tx.rollback();
            }
        }

        @Override
        public VertexPropertyFeatures properties() {
            return new HugeGraphVertexPropertyFeatures();
        }

        @Override
        public boolean supportsNumericIds() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return HugeGraphFeatures.this.graph.getConfiguration().allowVertexIdSetting();
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    private static class HugeGraphEdgeFeatures implements EdgeFeatures {
        @Override
        public EdgePropertyFeatures properties() {
            return new HugeGraphEdgePropertyFeatures();
        }

        @Override
        public boolean supportsCustomIds() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return false;
        }
    }

}
