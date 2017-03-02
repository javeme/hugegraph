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

package com.baidu.hugegraph.graphdb.query.vertex;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.baidu.hugegraph.core.HugeGraphVertex;
import com.baidu.hugegraph.core.VertexList;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import com.baidu.hugegraph.util.datastructures.IterablesUtil;

import java.util.*;

/**
 * An implementation of {@link VertexListInternal} that stores the actual vertex references
 * and simply wraps an {@link ArrayList} and keeps a boolean flag to remember whether this list is in sort order.
 *
 */
public class VertexArrayList implements VertexListInternal {

    public static final Comparator<HugeGraphVertex> VERTEX_ID_COMPARATOR = new Comparator<HugeGraphVertex>() {
        @Override
        public int compare(HugeGraphVertex o1, HugeGraphVertex o2) {
            return Long.compare(o1.longId(),o2.longId());
        }
    };

    private final StandardHugeGraphTx tx;
    private List<HugeGraphVertex> vertices;
    private boolean sorted;

    private VertexArrayList(StandardHugeGraphTx tx, List<HugeGraphVertex> vertices, boolean sorted) {
        Preconditions.checkArgument(tx!=null && vertices!=null);
        this.tx = tx;
        this.vertices=vertices;
        this.sorted=sorted;
    }

    public VertexArrayList(StandardHugeGraphTx tx) {
        Preconditions.checkNotNull(tx);
        this.tx=tx;
        vertices = new ArrayList<HugeGraphVertex>();
        sorted = true;
    }


    @Override
    public void add(HugeGraphVertex n) {
        if (!vertices.isEmpty()) sorted = sorted && (vertices.get(vertices.size()-1).longId()<=n.longId());
        vertices.add(n);
    }

    @Override
    public long getID(int pos) {
        return vertices.get(pos).longId();
    }

    @Override
    public LongArrayList getIDs() {
        return toLongList(vertices);
    }

    @Override
    public HugeGraphVertex get(int pos) {
        return vertices.get(pos);
    }

    @Override
    public void sort() {
        if (sorted) return;
        Collections.sort(vertices,VERTEX_ID_COMPARATOR);
        sorted = true;
    }

    @Override
    public boolean isSorted() {
        return sorted;
    }

    @Override
    public VertexList subList(int fromPosition, int length) {
        return new VertexArrayList(tx,vertices.subList(fromPosition,fromPosition+length),sorted);
    }

    @Override
    public int size() {
        return vertices.size();
    }

    @Override
    public void addAll(VertexList vertexlist) {
        Preconditions.checkArgument(vertexlist instanceof VertexArrayList, "Only supporting union of identical lists.");
        VertexArrayList other = (vertexlist instanceof VertexArrayList)?(VertexArrayList)vertexlist:
                ((VertexLongList)vertexlist).toVertexArrayList();
        if (sorted && other.isSorted()) {
            //Merge sort
            vertices = (ArrayList<HugeGraphVertex>) IterablesUtil.mergeSort(vertices, other.vertices, VERTEX_ID_COMPARATOR);
        } else {
            sorted = false;
            vertices.addAll(other.vertices);
        }
    }

    public VertexLongList toVertexLongList() {
        LongArrayList list = toLongList(vertices);
        return new VertexLongList(tx,list,sorted);
    }

    @Override
    public Iterator<HugeGraphVertex> iterator() {
        return Iterators.unmodifiableIterator(vertices.iterator());
    }

    /**
     * Utility method used to convert the list of vertices into a list of vertex ids (assuming all vertices have ids)
     *
     * @param vertices
     * @return
     */
    private static final LongArrayList toLongList(List<HugeGraphVertex> vertices) {
        LongArrayList result = new LongArrayList(vertices.size());
        for (HugeGraphVertex n : vertices) {
            result.add(n.longId());
        }
        return result;
    }

}
