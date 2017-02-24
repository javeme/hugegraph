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

import com.baidu.hugegraph.diskstorage.util.RecordIterator;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;
import com.baidu.hugegraph.graphdb.idmanagement.IDManager;
import com.baidu.hugegraph.graphdb.internal.InternalVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VertexIterable implements Iterable<InternalVertex> {

    private final StandardHugeGraphTx tx;
    private final StandardHugeGraph graph;

    public VertexIterable(final StandardHugeGraph graph, final StandardHugeGraphTx tx) {
        this.graph = graph;
        this.tx = tx;
    }

    @Override
    public Iterator<InternalVertex> iterator() {
        return new Iterator<InternalVertex>() {

            RecordIterator<Long> iterator = graph.getVertexIDs(tx.getTxHandle());
            InternalVertex nextVertex = nextVertex();

            private InternalVertex nextVertex() {
                InternalVertex v = null;
                while (v == null && iterator.hasNext()) {
                    long nextId = iterator.next().longValue();
                    // Filter out invisible vertices
                    if (IDManager.VertexIDType.Invisible.is(nextId))
                        continue;

                    v = tx.getInternalVertex(nextId);
                    // Filter out deleted vertices and types
                    if (v.isRemoved())
                        v = null;
                }
                return v;
            }

            @Override
            public boolean hasNext() {
                return nextVertex != null;
            }

            @Override
            public InternalVertex next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                InternalVertex returnVertex = nextVertex;
                nextVertex = nextVertex();
                return returnVertex;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
