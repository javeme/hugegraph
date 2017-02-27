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

package com.baidu.hugegraph.graphdb;

import com.google.common.collect.Iterables;
import com.baidu.hugegraph.core.HugeGraphFactory;
import com.baidu.hugegraph.core.HugeGraph;
import com.baidu.hugegraph.core.HugeGraphVertex;
import com.baidu.hugegraph.graphdb.query.vertex.VertexArrayList;
import com.baidu.hugegraph.graphdb.query.vertex.VertexLongList;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexListTest {

    @Test
    public void testLists() {

        int num = 13;

        HugeGraph g = HugeGraphFactory.open("inmemory");
        StandardHugeGraphTx tx = (StandardHugeGraphTx) g.newTransaction();
        VertexLongList vll = new VertexLongList(tx);
        VertexArrayList val = new VertexArrayList(tx);
        for (int i=0; i<num; i++) {
            HugeGraphVertex v = tx.addVertex();
            vll.add(v);
            val.add(v);
        }

        assertEquals(num, Iterables.size(vll));
        assertEquals(num, Iterables.size(val));

        vll.sort();
        val.sort();
        assertTrue(vll.isSorted());
        assertTrue(val.isSorted());

        for (Iterable<HugeGraphVertex> iterable : new Iterable[]{val,vll}) {
            Iterator<HugeGraphVertex> iter = iterable.iterator();
            HugeGraphVertex previous = null;
            for (int i = 0; i < num; i++) {
                HugeGraphVertex next = iter.next();
                if (previous!=null) assertTrue(previous.longId()<next.longId());
                previous = next;
            }
            try {
                iter.next();
                fail();
            } catch (NoSuchElementException ex) {

            }
        }


        tx.commit();
        g.close();

    }


}
