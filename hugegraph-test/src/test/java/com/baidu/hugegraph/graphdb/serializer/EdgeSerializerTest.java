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

package com.baidu.hugegraph.graphdb.serializer;

import com.baidu.hugegraph.StorageSetup;
import com.baidu.hugegraph.core.Multiplicity;
import com.baidu.hugegraph.core.HugeGraphEdge;
import com.baidu.hugegraph.core.HugeGraphVertex;
import com.baidu.hugegraph.core.schema.HugeGraphManagement;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;
import org.junit.Test;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class EdgeSerializerTest {

    @Test
    public void testValueOrdering() {
        StandardHugeGraph graph = (StandardHugeGraph) StorageSetup.getInMemoryGraph();
        HugeGraphManagement mgmt = graph.openManagement();
        mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        for (int i = 1; i <= 5; i++)
            mgmt.makePropertyKey("key" + i).dataType(Integer.class).make();
        mgmt.commit();

        HugeGraphVertex v1 = graph.addVertex(), v2 = graph.addVertex();
        HugeGraphEdge e1 = v1.addEdge("father", v2);
        for (int i = 1; i <= 5; i++)
            e1.property("key" + i, i);

        graph.tx().commit();

        e1.remove();
        graph.tx().commit();

    }

}
