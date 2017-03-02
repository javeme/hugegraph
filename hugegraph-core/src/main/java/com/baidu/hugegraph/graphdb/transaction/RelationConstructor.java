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

package com.baidu.hugegraph.graphdb.transaction;

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.core.EdgeLabel;
import com.baidu.hugegraph.core.PropertyKey;
import com.baidu.hugegraph.core.HugeGraphRelation;
import com.baidu.hugegraph.diskstorage.Entry;
import com.baidu.hugegraph.graphdb.database.EdgeSerializer;
import com.baidu.hugegraph.graphdb.internal.InternalRelation;
import com.baidu.hugegraph.graphdb.internal.InternalRelationType;
import com.baidu.hugegraph.graphdb.internal.InternalVertex;
import com.baidu.hugegraph.graphdb.relations.CacheEdge;
import com.baidu.hugegraph.graphdb.relations.CacheVertexProperty;
import com.baidu.hugegraph.graphdb.relations.RelationCache;
import com.baidu.hugegraph.graphdb.types.TypeInspector;
import com.baidu.hugegraph.graphdb.types.TypeUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Iterator;

/**
 */
public class RelationConstructor {

    public static RelationCache readRelationCache(Entry data, StandardHugeGraphTx tx) {
        return tx.getEdgeSerializer().readRelation(data, false, tx);
    }

    public static Iterable<HugeGraphRelation> readRelation(final InternalVertex vertex, final Iterable<Entry> data, final StandardHugeGraphTx tx) {
        return new Iterable<HugeGraphRelation>() {
            @Override
            public Iterator<HugeGraphRelation> iterator() {
                return new Iterator<HugeGraphRelation>() {

                    Iterator<Entry> iter = data.iterator();
                    HugeGraphRelation current = null;

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public HugeGraphRelation next() {
                        current = readRelation(vertex,iter.next(),tx);
                        return current;
                    }

                    @Override
                    public void remove() {
                        Preconditions.checkState(current!=null);
                        current.remove();
                    }
                };
            }
        };
    }

    public static InternalRelation readRelation(final InternalVertex vertex, final Entry data, final StandardHugeGraphTx tx) {
        RelationCache relation = tx.getEdgeSerializer().readRelation(data, true, tx);
        return readRelation(vertex,relation,data,tx,tx);
    }

    public static InternalRelation readRelation(final InternalVertex vertex, final Entry data,
                                                final EdgeSerializer serializer, final TypeInspector types,
                                                final VertexFactory vertexFac) {
        RelationCache relation = serializer.readRelation(data, true, types);
        return readRelation(vertex,relation,data,types,vertexFac);
    }


    private static InternalRelation readRelation(final InternalVertex vertex, final RelationCache relation,
                                         final Entry data, final TypeInspector types, final VertexFactory vertexFac) {
        InternalRelationType type = TypeUtil.getBaseType((InternalRelationType) types.getExistingRelationType(relation.typeId));

        if (type.isPropertyKey()) {
            assert relation.direction == Direction.OUT;
            return new CacheVertexProperty(relation.relationId, (PropertyKey) type, vertex, relation.getValue(), data);
        }

        if (type.isEdgeLabel()) {
            InternalVertex otherVertex = vertexFac.getInternalVertex(relation.getOtherVertexId());
            switch (relation.direction) {
                case IN:
                    return new CacheEdge(relation.relationId, (EdgeLabel) type, otherVertex, vertex, data);

                case OUT:
                    return new CacheEdge(relation.relationId, (EdgeLabel) type, vertex, otherVertex, data);

                default:
                    throw new AssertionError();
            }
        }

        throw new AssertionError();
    }

}
