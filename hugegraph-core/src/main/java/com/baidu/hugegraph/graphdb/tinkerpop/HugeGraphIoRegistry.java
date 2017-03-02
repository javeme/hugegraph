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

import com.baidu.hugegraph.core.attribute.Geoshape;
import com.baidu.hugegraph.graphdb.relations.RelationIdentifier;
import com.baidu.hugegraph.graphdb.tinkerpop.io.graphson.HugeGraphSONModule;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

/**
 */
public class HugeGraphIoRegistry extends AbstractIoRegistry {

    private static HugeGraphIoRegistry INSTANCE = new HugeGraphIoRegistry();

    // todo: made the constructor temporarily public to workaround an interoperability issue with hadoop in tp3 GA https://issues.apache.org/jira/browse/TINKERPOP3-771

    public HugeGraphIoRegistry() {
        register(GraphSONIo.class, null, HugeGraphSONModule.getInstance());
        register(GryoIo.class, RelationIdentifier.class, null);
        register(GryoIo.class, Geoshape.class, new Geoshape.GeoShapeGryoSerializer());
    }

    public static HugeGraphIoRegistry getInstance() {
        return INSTANCE;
    }
}
