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

package com.baidu.hugegraph.example;

import com.baidu.hugegraph.core.EdgeLabel;
import com.baidu.hugegraph.core.Multiplicity;
import com.baidu.hugegraph.core.PropertyKey;
import com.baidu.hugegraph.core.HugeGraphFactory;
import com.baidu.hugegraph.core.HugeGraph;
import com.baidu.hugegraph.core.HugeGraphTransaction;
import com.baidu.hugegraph.core.attribute.Geoshape;
import com.baidu.hugegraph.core.schema.ConsistencyModifier;
import com.baidu.hugegraph.core.schema.HugeGraphIndex;
import com.baidu.hugegraph.core.schema.HugeGraphManagement;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.io.File;

/**
 * Example Graph factory that creates a {@link HugeGraph} based on roman mythology.
 * Used in the documentation examples and tutorials.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphOfTheGodsFactory {

    public static final String INDEX_NAME = "search";

    public static HugeGraph create(final String directory) {
        HugeGraphFactory.Builder config = HugeGraphFactory.build();
        config.set("storage.backend", "berkeleyje");
        config.set("storage.directory", directory);
        config.set("index." + INDEX_NAME + ".backend", "elasticsearch");
        config.set("index." + INDEX_NAME + ".directory", directory + File.separator + "es");
        config.set("index." + INDEX_NAME + ".elasticsearch.local-mode", true);
        config.set("index." + INDEX_NAME + ".elasticsearch.client-only", false);

        HugeGraph graph = config.open();
        GraphOfTheGodsFactory.load(graph);
        return graph;
    }

    public static void loadWithoutMixedIndex(final HugeGraph graph, boolean uniqueNameCompositeIndex) {
        load(graph, null, uniqueNameCompositeIndex);
    }

    public static void load(final HugeGraph graph) {
        load(graph, INDEX_NAME, true);
    }

    public static void load(final HugeGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex) {

        //Create Schema
        HugeGraphManagement mgmt = graph.openManagement();
        final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        HugeGraphManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("name", Vertex.class).addKey(name);
        if (uniqueNameCompositeIndex)
            nameIndexBuilder.unique();
        HugeGraphIndex namei = nameIndexBuilder.buildCompositeIndex();
        mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
        final PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("vertices", Vertex.class).addKey(age).buildMixedIndex(mixedIndexName);

        final PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        final PropertyKey reason = mgmt.makePropertyKey("reason").dataType(String.class).make();
        final PropertyKey place = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("edges", Edge.class).addKey(reason).addKey(place).buildMixedIndex(mixedIndexName);

        mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel battled = mgmt.makeEdgeLabel("battled").signature(time).make();
        mgmt.buildEdgeIndex(battled, "battlesByTime", Direction.BOTH, Order.decr, time);
        mgmt.makeEdgeLabel("lives").signature(reason).make();
        mgmt.makeEdgeLabel("pet").make();
        mgmt.makeEdgeLabel("brother").make();

        mgmt.makeVertexLabel("hugegraph").make();
        mgmt.makeVertexLabel("location").make();
        mgmt.makeVertexLabel("god").make();
        mgmt.makeVertexLabel("demigod").make();
        mgmt.makeVertexLabel("human").make();
        mgmt.makeVertexLabel("monster").make();

        mgmt.commit();

        HugeGraphTransaction tx = graph.newTransaction();
        // vertices

        Vertex saturn = tx.addVertex(T.label, "hugegraph", "name", "saturn", "age", 10000);
        Vertex sky = tx.addVertex(T.label, "location", "name", "sky");
        Vertex sea = tx.addVertex(T.label, "location", "name", "sea");
        Vertex jupiter = tx.addVertex(T.label, "god", "name", "jupiter", "age", 5000);
        Vertex neptune = tx.addVertex(T.label, "god", "name", "neptune", "age", 4500);
        Vertex hercules = tx.addVertex(T.label, "demigod", "name", "hercules", "age", 30);
        Vertex alcmene = tx.addVertex(T.label, "human", "name", "alcmene", "age", 45);
        Vertex pluto = tx.addVertex(T.label, "god", "name", "pluto", "age", 4000);
        Vertex nemean = tx.addVertex(T.label, "monster", "name", "nemean");
        Vertex hydra = tx.addVertex(T.label, "monster", "name", "hydra");
        Vertex cerberus = tx.addVertex(T.label, "monster", "name", "cerberus");
        Vertex tartarus = tx.addVertex(T.label, "location", "name", "tartarus");

        // edges

        jupiter.addEdge("father", saturn);
        jupiter.addEdge("lives", sky, "reason", "loves fresh breezes");
        jupiter.addEdge("brother", neptune);
        jupiter.addEdge("brother", pluto);

        neptune.addEdge("lives", sea).property("reason", "loves waves");
        neptune.addEdge("brother", jupiter);
        neptune.addEdge("brother", pluto);

        hercules.addEdge("father", jupiter);
        hercules.addEdge("mother", alcmene);
        hercules.addEdge("battled", nemean, "time", 1, "place", Geoshape.point(38.1f, 23.7f));
        hercules.addEdge("battled", hydra, "time", 2, "place", Geoshape.point(37.7f, 23.9f));
        hercules.addEdge("battled", cerberus, "time", 12, "place", Geoshape.point(39f, 22f));

        pluto.addEdge("brother", jupiter);
        pluto.addEdge("brother", neptune);
        pluto.addEdge("lives", tartarus, "reason", "no fear of death");
        pluto.addEdge("pet", cerberus);

        cerberus.addEdge("lives", tartarus);

        // commit the transaction to disk
        tx.commit();
    }

    /**
     * Calls {@link HugeGraphFactory#open(String)}, passing the HugeGraph configuration file path
     * which must be the sole element in the {@code args} array, then calls
     * {@link #load(com.baidu.hugegraph.core.HugeGraph)} on the opened graph,
     * then calls {@link com.baidu.hugegraph.core.HugeGraph#close()}
     * and returns.
     * <p/>
     * This method may call {@link System#exit(int)} if it encounters an error, such as
     * failure to parse its arguments.  Only use this method when executing main from
     * a command line.  Use one of the other methods on this class ({@link #create(String)}
     * or {@link #load(com.baidu.hugegraph.core.HugeGraph)}) when calling from
     * an enclosing application.
     *
     * @param args a singleton array containing a path to a HugeGraph config properties file
     */
    public static void main(String args[]) {
        if (null == args || 1 != args.length) {
            System.err.println("Usage: GraphOfTheGodsFactory <hugegraph-config-file>");
            System.exit(1);
        }

        HugeGraph g = HugeGraphFactory.open(args[0]);
        load(g);
        g.close();
    }
}
