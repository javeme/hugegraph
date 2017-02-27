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

package com.baidu.hugegraph.graphdb.types;

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.core.VertexLabel;
import com.baidu.hugegraph.core.schema.VertexLabelMaker;
import com.baidu.hugegraph.graphdb.internal.HugeGraphSchemaCategory;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import com.baidu.hugegraph.graphdb.types.system.SystemTypeManager;
import static com.baidu.hugegraph.graphdb.types.TypeDefinitionCategory.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardVertexLabelMaker implements VertexLabelMaker {

    private final StandardHugeGraphTx tx;

    private String name;
    private boolean partitioned;
    private boolean isStatic;

    public StandardVertexLabelMaker(StandardHugeGraphTx tx) {
        this.tx = tx;
    }

    public StandardVertexLabelMaker name(String name) {
        //Verify name
        SystemTypeManager.isNotSystemName(HugeGraphSchemaCategory.VERTEXLABEL, name);
        this.name=name;
        return this;
    }


    public String getName() {
        return name;
    }

    @Override
    public StandardVertexLabelMaker partition() {
        partitioned=true;
        return this;
    }

    @Override
    public StandardVertexLabelMaker setStatic() {
        isStatic = true;
        return this;
    }

    @Override
    public VertexLabel make() {
        Preconditions.checkArgument(!partitioned || !isStatic,"A vertex label cannot be partitioned and static at the same time");
        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(PARTITIONED, partitioned);
        def.setValue(STATIC, isStatic);

        return (VertexLabelVertex)tx.makeSchemaVertex(HugeGraphSchemaCategory.VERTEXLABEL,name,def);
    }
}
