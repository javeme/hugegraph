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

package com.baidu.hugegraph.graphdb.inmemory;

import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.WriteConfiguration;
import com.baidu.hugegraph.graphdb.HugeGraphPartitionGraphTest;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryPartitionGraphTest extends HugeGraphPartitionGraphTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(GraphDatabaseConfiguration.IDS_FLUSH,false);
        return config.getConfiguration();
    }

    @Override
    public void clopen(Object... settings) {
        newTx();
    }

    @Override
    public void testPartitionSpreadFlushBatch() {
    }

    @Override
    public void testPartitionSpreadFlushNoBatch() {
    }

    @Override
    public void testKeybasedGraphPartitioning() {}

}
