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

package com.baidu.hugegraph.blueprints;

import com.baidu.hugegraph.core.Cardinality;
import com.baidu.hugegraph.core.EdgeLabel;
import com.baidu.hugegraph.core.PropertyKey;
import com.baidu.hugegraph.core.HugeGraphFactory;
import com.baidu.hugegraph.core.HugeGraph;
import com.baidu.hugegraph.core.VertexLabel;
import com.baidu.hugegraph.core.schema.HugeGraphManagement;
import com.baidu.hugegraph.diskstorage.configuration.BasicConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.ConfigElement;
import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.WriteConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.backend.CommonsConfiguration;
import com.baidu.hugegraph.graphdb.HugeGraphBaseTest;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;
import com.baidu.hugegraph.graphdb.olap.computer.FulgoraElementTraversal;
import com.baidu.hugegraph.graphdb.olap.computer.FulgoraVertexProperty;
import com.baidu.hugegraph.graphdb.relations.CacheEdge;
import com.baidu.hugegraph.graphdb.relations.CacheVertexProperty;
import com.baidu.hugegraph.graphdb.relations.SimpleHugeGraphProperty;
import com.baidu.hugegraph.graphdb.relations.StandardEdge;
import com.baidu.hugegraph.graphdb.relations.StandardVertexProperty;
import com.baidu.hugegraph.graphdb.tinkerpop.HugeGraphVariables;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import com.baidu.hugegraph.graphdb.types.VertexLabelVertex;
import com.baidu.hugegraph.graphdb.types.system.EmptyVertex;
import com.baidu.hugegraph.graphdb.types.vertices.EdgeLabelVertex;
import com.baidu.hugegraph.graphdb.types.vertices.PropertyKeyVertex;
import com.baidu.hugegraph.graphdb.types.vertices.HugeGraphSchemaVertex;
import com.baidu.hugegraph.graphdb.vertices.CacheVertex;
import com.baidu.hugegraph.graphdb.vertices.PreloadedVertex;
import com.baidu.hugegraph.graphdb.vertices.StandardVertex;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.TransactionTest;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractHugeGraphProvider extends AbstractGraphProvider {

    private static final Logger logger = LoggerFactory.getLogger(AbstractHugeGraphProvider.class);

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {
        {
            add(StandardHugeGraph.class);
            add(StandardHugeGraphTx.class);

            add(StandardVertex.class);
            add(CacheVertex.class);
            add(PreloadedVertex.class);
            add(EdgeLabelVertex.class);
            add(PropertyKeyVertex.class);
            add(VertexLabelVertex.class);
            add(HugeGraphSchemaVertex.class);
            add(EmptyVertex.class);

            add(StandardEdge.class);
            add(CacheEdge.class);
            add(EdgeLabel.class);
            add(EdgeLabelVertex.class);

            add(StandardVertexProperty.class);
            add(CacheVertexProperty.class);
            add(SimpleHugeGraphProperty.class);
            add(CacheVertexProperty.class);
            add(FulgoraVertexProperty.class);

            add(HugeGraphVariables.class);

            add(FulgoraElementTraversal.class);
        }
    };

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        return GraphTraversalSource.standard().create(graph);
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph, final TraversalStrategy...strategies) {
        final GraphTraversalSource.Builder builder =
                GraphTraversalSource.build().engine(StandardTraversalEngine.build());
        Stream.of(strategies).forEach(builder::with);
        return builder.create(graph);
    }

    // @Override
    // public <ID> ID reconstituteGraphSONIdentifier(final Class<? extends Element> clazz, final Object id) {
    // if (Edge.class.isAssignableFrom(clazz)) {
    // // HugeGraphSONModule toStrings the edgeid - expect a String value for the id
    // if (!(id instanceof String)) throw new RuntimeException("Expected a String value for the RelationIdentifier");
    // return (ID) RelationIdentifier.parse((String) id);
    // } else {
    // return (ID) id;
    // }
    // }

    @Override
    public void clear(Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
            while (g instanceof WrappedGraph)
                g = ((WrappedGraph<? extends Graph>) g).getBaseGraph();
            HugeGraph graph = (HugeGraph) g;
            if (graph.isOpen()) {
                if (g.tx().isOpen())
                    g.tx().rollback();
                try {
                    g.close();
                } catch (IOException | IllegalStateException e) {
                    logger.warn("Titan graph may not have closed cleanly", e);
                }
            }
        }

        WriteConfiguration config = new CommonsConfiguration(configuration);
        BasicConfiguration readConfig =
                new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.NONE);
        if (readConfig.has(GraphDatabaseConfiguration.STORAGE_BACKEND)) {
            HugeGraphBaseTest.clearGraph(config);
        }
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName,
            final LoadGraphWith.GraphData loadGraphWith) {
        ModifiableConfiguration conf = getHugeGraphConfiguration(graphName, test, testMethodName);
        conf.set(GraphDatabaseConfiguration.COMPUTER_RESULT_MODE, "persist");
        conf.set(GraphDatabaseConfiguration.AUTO_TYPE, "tp3");
        Map<String, Object> result = new HashMap<>();
        conf.getAll().entrySet().stream().forEach(
                e -> result.put(ConfigElement.getPath(e.getKey().element, e.getKey().umbrellaElements), e.getValue()));
        result.put(Graph.GRAPH, HugeGraphFactory.class.getName());
        return result;
    }

    public abstract ModifiableConfiguration getHugeGraphConfiguration(String graphName, Class<?> test,
            String testMethodName);

    @Override
    public void loadGraphData(final Graph g, final LoadGraphWith loadGraphWith, final Class testClass,
            final String testName) {
        if (loadGraphWith != null) {
            this.createIndices((HugeGraph) g, loadGraphWith.value());
        } else {
            if (TransactionTest.class.equals(testClass)
                    && testName.equalsIgnoreCase("shouldExecuteWithCompetingThreads")) {
                HugeGraphManagement mgmt = ((HugeGraph) g).openManagement();
                mgmt.makePropertyKey("blah").dataType(Double.class).make();
                mgmt.makePropertyKey("bloop").dataType(Integer.class).make();
                mgmt.makePropertyKey("test").dataType(Object.class).make();
                mgmt.makeEdgeLabel("friend").make();
                mgmt.commit();
            }
        }
        super.loadGraphData(g, loadGraphWith, testClass, testName);
    }

    private void createIndices(final HugeGraph g, final LoadGraphWith.GraphData graphData) {
        HugeGraphManagement mgmt = g.openManagement();
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            VertexLabel artist = mgmt.makeVertexLabel("artist").make();
            VertexLabel song = mgmt.makeVertexLabel("song").make();

            PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey songType =
                    mgmt.makePropertyKey("songType").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey performances =
                    mgmt.makePropertyKey("performances").cardinality(Cardinality.LIST).dataType(Integer.class).make();

            mgmt.buildIndex("artistByName", Vertex.class).addKey(name).indexOnly(artist).buildCompositeIndex();
            mgmt.buildIndex("songByName", Vertex.class).addKey(name).indexOnly(song).buildCompositeIndex();
            mgmt.buildIndex("songByType", Vertex.class).addKey(songType).indexOnly(song).buildCompositeIndex();
            mgmt.buildIndex("songByPerformances", Vertex.class).addKey(performances).indexOnly(song)
                    .buildCompositeIndex();

        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            VertexLabel person = mgmt.makeVertexLabel("person").make();
            VertexLabel software = mgmt.makeVertexLabel("software").make();

            PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey lang = mgmt.makePropertyKey("lang").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey age = mgmt.makePropertyKey("age").cardinality(Cardinality.LIST).dataType(Integer.class).make();

            mgmt.buildIndex("personByName", Vertex.class).addKey(name).indexOnly(person).buildCompositeIndex();
            mgmt.buildIndex("softwareByName", Vertex.class).addKey(name).indexOnly(software).buildCompositeIndex();
            mgmt.buildIndex("personByAge", Vertex.class).addKey(age).indexOnly(person).buildCompositeIndex();
            mgmt.buildIndex("softwareByLang", Vertex.class).addKey(lang).indexOnly(software).buildCompositeIndex();

        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey lang = mgmt.makePropertyKey("lang").cardinality(Cardinality.LIST).dataType(String.class).make();
            PropertyKey age = mgmt.makePropertyKey("age").cardinality(Cardinality.LIST).dataType(Integer.class).make();

            mgmt.buildIndex("byName", Vertex.class).addKey(name).buildCompositeIndex();
            mgmt.buildIndex("byAge", Vertex.class).addKey(age).buildCompositeIndex();
            mgmt.buildIndex("byLang", Vertex.class).addKey(lang).buildCompositeIndex();

        } else {
            // TODO: add CREW work here.
            // TODO: add meta_property indices when meta_property graph is provided
            // throw new RuntimeException("Could not load graph with " + graphData);
        }
        mgmt.commit();
    }

}
