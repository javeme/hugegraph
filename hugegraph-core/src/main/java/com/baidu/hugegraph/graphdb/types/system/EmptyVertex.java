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

package com.baidu.hugegraph.graphdb.types.system;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.baidu.hugegraph.core.*;
import com.baidu.hugegraph.diskstorage.EntryList;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.SliceQuery;
import com.baidu.hugegraph.graphdb.internal.ElementLifeCycle;
import com.baidu.hugegraph.graphdb.internal.InternalRelation;
import com.baidu.hugegraph.graphdb.internal.InternalVertex;
import com.baidu.hugegraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import com.baidu.hugegraph.util.datastructures.Retriever;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;
import java.util.List;

public class EmptyVertex implements InternalVertex {

    private static final String errorName = "Empty vertex";

    /*
     * --------------------------------------------------------------- HugeGraphRelation Iteration/Access
     * ---------------------------------------------------------------
     */

    @Override
    public VertexCentricQueryBuilder query() {
        throw new UnsupportedOperationException(errorName + " do not support querying");
    }

    @Override
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public EntryList loadRelations(SliceQuery query, Retriever<SliceQuery, EntryList> lookup) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery query) {
        return false;
    }

    @Override
    public boolean hasRemovedRelations() {
        return false;
    }

    @Override
    public boolean hasAddedRelations() {
        return false;
    }

    @Override
    public String label() {
        return vertexLabel().name();
    }

    @Override
    public VertexLabel vertexLabel() {
        return BaseVertexLabel.DEFAULT_VERTEXLABEL;
    }

    @Override
    public <O> O valueOrNull(PropertyKey key) {
        if (key instanceof ImplicitKey)
            return ((ImplicitKey) key).computeProperty(this);
        return null;
    }

    @Override
    public <O> O value(String key) {
        if (!tx().containsPropertyKey(key))
            throw Property.Exceptions.propertyDoesNotExist(this, key);
        O val = valueOrNull(tx().getPropertyKey(key));
        if (val == null)
            throw Property.Exceptions.propertyDoesNotExist(this, key);
        return val;
    }

    /*
     * --------------------------------------------------------------- Convenience Methods for HugeGraphElement Creation
     * ---------------------------------------------------------------
     */

    @Override
    public <V> HugeGraphVertexProperty<V> property(String key, V value, Object...keyValues) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }

    @Override
    public <V> HugeGraphVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value,
            Object...keyValues) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String...edgeLabels) {
        return Iterators.emptyIterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String...edgeLabels) {
        return Iterators.emptyIterator();
    }

    @Override
    public boolean addRelation(InternalRelation e) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public void removeRelation(InternalRelation e) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public HugeGraphEdge addEdge(String s, Vertex vertex, Object...keyValues) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    /*
     * --------------------------------------------------------------- In Memory HugeGraphElement
     * ---------------------------------------------------------------
     */

    @Override
    public long longId() {
        throw new UnsupportedOperationException(errorName + " don't have an ID");
    }

    @Override
    public Object id() {
        return hasId() ? longId() : null;
    }

    @Override
    public boolean hasId() {
        return false;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(errorName + " cannot be removed");
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String...propertyKeys) {
        return Iterators.emptyIterator();
    }

    @Override
    public void setId(long id) {
        throw new UnsupportedOperationException(errorName + " don't have an id");
    }

    @Override
    public byte getLifeCycle() {
        return ElementLifeCycle.Loaded;
    }

    @Override
    public boolean isInvisible() {
        return false;
    }

    @Override
    public boolean isRemoved() {
        return false;
    }

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public boolean isModified() {
        return false;
    }

    @Override
    public boolean isNew() {
        return false;
    }

    @Override
    public InternalVertex it() {
        return this;
    }

    @Override
    public StandardHugeGraphTx tx() {
        throw new UnsupportedOperationException(errorName + " don't have an associated transaction");
    }

}
