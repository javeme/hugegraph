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

package com.baidu.hugegraph.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.baidu.hugegraph.core.*;
import com.baidu.hugegraph.graphdb.internal.AbstractElement;
import com.baidu.hugegraph.graphdb.internal.ElementLifeCycle;
import com.baidu.hugegraph.graphdb.internal.InternalVertex;
import com.baidu.hugegraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import com.baidu.hugegraph.graphdb.types.VertexLabelVertex;
import com.baidu.hugegraph.graphdb.types.system.BaseLabel;
import com.baidu.hugegraph.graphdb.types.system.BaseVertexLabel;
import com.baidu.hugegraph.graphdb.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

public abstract class AbstractVertex extends AbstractElement implements InternalVertex, Vertex {

    private final StandardHugeGraphTx tx;

    protected AbstractVertex(StandardHugeGraphTx tx, long id) {
        super(id);
        assert tx != null;
        this.tx = tx;
    }

    @Override
    public final InternalVertex it() {
        if (tx.isOpen())
            return this;

        InternalVertex next = (InternalVertex) tx.getNextTx().getVertex(longId());
        if (next == null)
            throw InvalidElementException.removedException(this);
        else
            return next;
    }

    @Override
    public final StandardHugeGraphTx tx() {
        return tx.isOpen() ? tx : tx.getNextTx();
    }

    @Override
    public long getCompareId() {
        if (tx.isPartitionedVertex(this))
            return tx.getIdInspector().getCanonicalVertexId(longId());
        else
            return longId();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Object id() {
        return longId();
    }

    @Override
    public boolean isModified() {
        return ElementLifeCycle.isModified(it().getLifeCycle());
    }

    protected final void verifyAccess() {
        if (isRemoved()) {
            throw InvalidElementException.removedException(this);
        }
    }

    /*
     * --------------------------------------------------------------- Changing Edges
     * ---------------------------------------------------------------
     */

    @Override
    public synchronized void remove() {
        verifyAccess();
        // if (isRemoved()) return; //Remove() is idempotent
        Iterator<HugeGraphRelation> iter = it().query().noPartitionRestriction().relations().iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        // Remove all system types on the vertex
        for (HugeGraphRelation r : it().query().noPartitionRestriction().system().relations()) {
            r.remove();
        }
    }

    /*
     * --------------------------------------------------------------- HugeGraphRelation Iteration/Access
     * ---------------------------------------------------------------
     */

    @Override
    public String label() {
        return vertexLabel().name();
    }

    protected Vertex getVertexLabelInternal() {
        return Iterables.getOnlyElement(tx().query(this).noPartitionRestriction().type(BaseLabel.VertexLabelEdge)
                .direction(Direction.OUT).vertices(), null);
    }

    @Override
    public VertexLabel vertexLabel() {
        Vertex label = getVertexLabelInternal();
        if (label == null)
            return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        else
            return (VertexLabelVertex) label;
    }

    @Override
    public VertexCentricQueryBuilder query() {
        verifyAccess();
        return tx().query(this);
    }

    @Override
    public <O> O valueOrNull(PropertyKey key) {
        return (O) property(key.name()).orElse(null);
    }

    /*
     * --------------------------------------------------------------- Convenience Methods for HugeGraphElement Creation
     * ---------------------------------------------------------------
     */

    public <V> HugeGraphVertexProperty<V> property(final String key, final V value, final Object...keyValues) {
        HugeGraphVertexProperty<V> p = tx().addProperty(it(), tx().getOrCreatePropertyKey(key), value);
        ElementHelper.attachProperties(p, keyValues);
        return p;
    }

    @Override
    public <V> HugeGraphVertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key,
            final V value, final Object...keyValues) {
        HugeGraphVertexProperty<V> p = tx().addProperty(cardinality, it(), tx().getOrCreatePropertyKey(key), value);
        ElementHelper.attachProperties(p, keyValues);
        return p;
    }

    @Override
    public HugeGraphEdge addEdge(String label, Vertex vertex, Object...keyValues) {
        Preconditions.checkArgument(vertex instanceof HugeGraphVertex, "Invalid vertex provided: %s", vertex);
        HugeGraphEdge edge = tx().addEdge(it(), (HugeGraphVertex) vertex, tx().getOrCreateEdgeLabel(label));
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    public Iterator<Edge> edges(Direction direction, String...labels) {
        return (Iterator) query().direction(direction).labels(labels).edges().iterator();
    }

    public <V> Iterator<VertexProperty<V>> properties(String...keys) {
        return (Iterator) query().direction(Direction.OUT).keys(keys).properties().iterator();
    }

    public Iterator<Vertex> vertices(final Direction direction, final String...edgeLabels) {
        return (Iterator) query().direction(direction).labels(edgeLabels).vertices().iterator();

    }

}
