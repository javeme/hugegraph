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

package com.baidu.hugegraph.graphdb.tinkerpop.plugin;

import com.baidu.hugegraph.core.Cardinality;
import com.baidu.hugegraph.core.Multiplicity;
import com.baidu.hugegraph.core.attribute.Geo;
import com.baidu.hugegraph.core.attribute.Text;
import com.baidu.hugegraph.example.GraphOfTheGodsFactory;
import com.baidu.hugegraph.graphdb.tinkerpop.HugeGraphIoRegistry;
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HugeGraphGremlinPlugin implements GremlinPlugin {

    private static final String IMPORT = "import ";
    private static final String IMPORT_STATIC = IMPORT + "static ";
    private static final String DOT_STAR = ".*";

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT + "com.baidu.hugegraph.core" + DOT_STAR);
        add(IMPORT + "com.baidu.hugegraph.core.attribute" + DOT_STAR);
        add(IMPORT + "com.baidu.hugegraph.core.schema" + DOT_STAR);
        add(IMPORT + GraphOfTheGodsFactory.class.getName());
        add(IMPORT + "com.baidu.hugegraph.hadoop.MapReduceIndexManagement");
        add(IMPORT + "java.time" + DOT_STAR);
        add(IMPORT + HugeGraphIoRegistry.class.getName());

        // Static imports on enum values used in query constraint expressions
        add(IMPORT_STATIC + Geo.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + Text.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + Multiplicity.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + Cardinality.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + ChronoUnit.class.getName() + DOT_STAR);

    }};

    @Override
    public String getName() {
        return "hugegraph.imports";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

}
