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

package com.baidu.hugegraph.diskstorage.cassandra;

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.diskstorage.BaseTransactionConfig;
import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.diskstorage.util.StandardBaseTransactionConfig;
import com.baidu.hugegraph.diskstorage.util.time.TimestampProviders;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;

import static com.baidu.hugegraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY;
import static com.baidu.hugegraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY;
import static org.junit.Assert.*;

public class CassandraTransactionTest {

    /*
     * testRead/WriteConsistencyLevel have unnecessary code duplication that could be avoided by creating a common
     * helper method that takes a ConfigOption parameter and a function that converts a CassandraTransaction to a
     * consistency level by calling either ct.getReadConsistencyLevel() or .getWriteConsistencyLevel(), but it doesn't
     * seem worth the complexity.
     */

    @Test
    public void testWriteConsistencyLevel() {
        int levelsChecked = 0;

        // Test whether CassandraTransaction honors the write consistency level option
        for (CLevel writeLevel : CLevel.values()) {
            StandardBaseTransactionConfig.Builder b = new StandardBaseTransactionConfig.Builder();
            ModifiableConfiguration mc = GraphDatabaseConfiguration.buildGraphConfiguration();
            mc.set(CASSANDRA_WRITE_CONSISTENCY, writeLevel.name());
            b.customOptions(mc);
            b.timestampProvider(TimestampProviders.MICRO);
            CassandraTransaction ct = new CassandraTransaction(b.build());
            assertEquals(writeLevel, ct.getWriteConsistencyLevel());
            levelsChecked++;
        }

        // Sanity check: if CLevel.values was empty, something is wrong with the test
        Preconditions.checkState(0 < levelsChecked);
    }

    @Test
    public void testReadConsistencyLevel() {
        int levelsChecked = 0;

        // Test whether CassandraTransaction honors the write consistency level option
        for (CLevel writeLevel : CLevel.values()) {
            StandardBaseTransactionConfig.Builder b = new StandardBaseTransactionConfig.Builder();
            ModifiableConfiguration mc = GraphDatabaseConfiguration.buildGraphConfiguration();
            mc.set(CASSANDRA_READ_CONSISTENCY, writeLevel.name());
            b.timestampProvider(TimestampProviders.MICRO);
            b.customOptions(mc);
            CassandraTransaction ct = new CassandraTransaction(b.build());
            assertEquals(writeLevel, ct.getReadConsistencyLevel());
            levelsChecked++;
        }

        // Sanity check: if CLevel.values was empty, something is wrong with the test
        Preconditions.checkState(0 < levelsChecked);
    }

    @Test
    public void testTimestampProvider() {
        BaseTransactionConfig txcfg = StandardBaseTransactionConfig.of(TimestampProviders.NANO);
        CassandraTransaction ct = new CassandraTransaction(txcfg);
        assertEquals(TimestampProviders.NANO, ct.getConfiguration().getTimestampProvider());

        txcfg = StandardBaseTransactionConfig.of(TimestampProviders.MICRO);
        ct = new CassandraTransaction(txcfg);
        assertEquals(TimestampProviders.MICRO, ct.getConfiguration().getTimestampProvider());

        txcfg = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        ct = new CassandraTransaction(txcfg);
        assertEquals(TimestampProviders.MILLI, ct.getConfiguration().getTimestampProvider());
    }
}
