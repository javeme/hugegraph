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

import com.baidu.hugegraph.CassandraStorageSetup;
import com.baidu.hugegraph.core.HugeGraphFactory;
import com.baidu.hugegraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.baidu.hugegraph.diskstorage.configuration.ConfigElement;
import com.baidu.hugegraph.diskstorage.configuration.WriteConfiguration;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static com.baidu.hugegraph.diskstorage.cassandra.AbstractCassandraStoreManager.*;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class CassandraGraphTest extends HugeGraphTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    protected boolean isLockingOptimistic() {
        return true;
    }

    @Test
    public void testHasTTL() throws Exception {
        assertTrue(features.hasCellTTL());
    }

    @Test
    public void testGraphConfigUsedByThreadBoundTx() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(CASSANDRA_READ_CONSISTENCY), "ALL");
        wc.set(ConfigElement.getPath(CASSANDRA_WRITE_CONSISTENCY), "LOCAL_QUORUM");

        graph = (StandardHugeGraph) HugeGraphFactory.open(wc);

        StandardHugeGraphTx tx = (StandardHugeGraphTx)graph.getCurrentThreadTx();
        assertEquals("ALL",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY));
        assertEquals("LOCAL_QUORUM",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY));
    }

    @Test
    public void testGraphConfigUsedByTx() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(CASSANDRA_READ_CONSISTENCY), "TWO");
        wc.set(ConfigElement.getPath(CASSANDRA_WRITE_CONSISTENCY), "THREE");

        graph = (StandardHugeGraph) HugeGraphFactory.open(wc);

        StandardHugeGraphTx tx = (StandardHugeGraphTx)graph.newTransaction();
        assertEquals("TWO",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY));
        assertEquals("THREE",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY));
        tx.rollback();
    }

    @Test
    public void testCustomConfigUsedByTx() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(CASSANDRA_READ_CONSISTENCY), "ALL");
        wc.set(ConfigElement.getPath(CASSANDRA_WRITE_CONSISTENCY), "ALL");

        graph = (StandardHugeGraph) HugeGraphFactory.open(wc);

        StandardHugeGraphTx tx = (StandardHugeGraphTx)graph.buildTransaction()
                .customOption(ConfigElement.getPath(CASSANDRA_READ_CONSISTENCY), "ONE")
                .customOption(ConfigElement.getPath(CASSANDRA_WRITE_CONSISTENCY), "TWO").start();

        assertEquals("ONE",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY));
        assertEquals("TWO",
                tx.getTxHandle().getBaseTransactionConfig().getCustomOptions()
                        .get(AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY));
        tx.rollback();
    }
}
