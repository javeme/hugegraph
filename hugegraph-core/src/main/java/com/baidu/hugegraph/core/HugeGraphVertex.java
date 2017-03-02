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


package com.baidu.hugegraph.core;


import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * HugeGraphVertex is the basic unit of a {@link HugeGraph}.
 * It extends the functionality provided by Blueprint's {@link Vertex} by helper and convenience methods.
 * <p />
 * Vertices have incident edges and properties. Edge connect the vertex to other vertices. Properties attach key-value
 * pairs to this vertex to define it.
 * <p />
 * Like {@link HugeGraphRelation} a vertex has a vertex label.
 *
 */
public interface HugeGraphVertex extends HugeGraphElement, Vertex {

    /* ---------------------------------------------------------------
      * Creation and modification methods
      * ---------------------------------------------------------------
      */

    /**
     * Creates a new edge incident on this vertex.
     * <p/>
     * Creates and returns a new {@link HugeGraphEdge} of the specified label with this vertex being the outgoing vertex
     * and the given vertex being the incoming vertex.
     * <br />
     * Automatically creates the edge label if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param label  label of the edge to be created
     * @param vertex incoming vertex of the edge to be created
     * @return new edge
     */
    @Override
    public HugeGraphEdge addEdge(String label, Vertex vertex, Object... keyValues);

    /**
     * Creates a new property for this vertex and given key with the specified value.
     * <p/>
     * Creates and returns a new {@link HugeGraphVertexProperty} for the given key on this vertex with the specified
     * object being the value.
     * <br />
     * Automatically creates the property key if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an {@link IllegalArgumentException}.
     *
     * @param key       key of the property to be created
     * @param value value of the property to be created
     * @return New property
     * @throws IllegalArgumentException if the value does not match the data type of the property key.
     */
    @Override
    public default<V> HugeGraphVertexProperty<V> property(String key, V value) {
        return this.property(key, value, EMPTY_ARGS);
    }

    @Override
    public <V> HugeGraphVertexProperty<V> property(final String key, final V value, final Object... keyValues);


    @Override
    public <V> HugeGraphVertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues);

     /* ---------------------------------------------------------------
      * Vertex Label
      * ---------------------------------------------------------------
      */

    /**
     * Returns the name of the vertex label for this vertex.
     *
     * @return
     */
    @Override
    public default String label() {
        return vertexLabel().name();
    }

    /**
     * Returns the vertex label of this vertex.
     *
     * @return
     */
    public VertexLabel vertexLabel();

	/* ---------------------------------------------------------------
     * Incident HugeGraphRelation Access methods
	 * ---------------------------------------------------------------
	 */

    /**
     * Starts a new {@link HugeGraphVertexQuery} for this vertex.
     * <p/>
     * Initializes and returns a new {@link HugeGraphVertexQuery} based on this vertex.
     *
     * @return New HugeGraphQuery for this vertex
     * @see HugeGraphVertexQuery
     */
    public HugeGraphVertexQuery<? extends HugeGraphVertexQuery> query();

    /**
     * Checks whether this entity has been loaded into the current transaction and modified.
     *
     * @return True, has been loaded and modified, else false.
     */
    public boolean isModified();


}
