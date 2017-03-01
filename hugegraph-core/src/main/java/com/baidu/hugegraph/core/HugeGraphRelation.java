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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * HugeGraphRelation is the most abstract form of a relation between a vertex and some other entity, where
 * relation is understood in its mathematical sense. It generalizes the notion of an edge and a property.
 * <br />
 * A HugeGraphRelation extends {@link HugeGraphElement} which means it is an entity in its own right. This means, a HugeGraphRelation
 * can have properties and unidirectional edges connecting it to other vertices.
 * <br />
 * A HugeGraphRelation is an abstract concept. A HugeGraphRelation is either a {@link HugeGraphVertexProperty} or a {@link HugeGraphEdge}.
 * A HugeGraphRelation has a type which is either a label or key depending on the implementation.
 * <br />
 * A HugeGraphRelation is either directed, or unidirected. Properties are always directed (connecting a vertex
 * with a value). A unidirected edge is a special type of directed edge where the connection is only established from the
 * perspective of the outgoing vertex. In that sense, a unidirected edge is akin to a link.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see HugeGraphEdge
 * @see HugeGraphVertexProperty
 */
public interface HugeGraphRelation extends HugeGraphElement {

    /**
     * Retrieves the value associated with the given key on this vertex and casts it to the specified type.
     * If the key has cardinality SINGLE, then there can be at most one value and this value is returned (or null).
     * Otherwise a list of all associated values is returned, or an empty list if non exist.
     * <p/>
     *
     * @param key string identifying a key
     * @return value or list of values associated with key
     */
    public <V> V value(String key);

    /**
     * Returns the type of this relation.
     * <p/>
     * The type is either a label ({@link EdgeLabel} if this relation is an edge or a key ({@link PropertyKey}) if this
     * relation is a property.
     *
     * @return Type of this relation
     */
    public RelationType getType();

    /**
     * Returns the direction of this relation from the perspective of the specified vertex.
     *
     * @param vertex vertex on which the relation is incident
     * @return The direction of this relation from the perspective of the specified vertex.
     * @throws InvalidElementException if this relation is not incident on the vertex
     */
    public Direction direction(Vertex vertex);

    /**
     * Checks whether this relation is incident on the specified vertex.
     *
     * @param vertex vertex to check incidence for
     * @return true, if this relation is incident on the vertex, else false
     */
    public boolean isIncidentOn(Vertex vertex);

    /**
     * Checks whether this relation is a loop.
     * An relation is a loop if it connects a vertex with itself.
     *
     * @return true, if this relation is a loop, else false.
     */
    boolean isLoop();

    /**
     * Checks whether this relation is a property.
     *
     * @return true, if this relation is a property, else false.
     * @see HugeGraphVertexProperty
     */
    boolean isProperty();

    /**
     * Checks whether this relation is an edge.
     *
     * @return true, if this relation is an edge, else false.
     * @see HugeGraphEdge
     */
    boolean isEdge();


}
