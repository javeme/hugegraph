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

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.WriteConfiguration;
import com.baidu.hugegraph.graphdb.HugeGraphTest;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Test;

import java.util.Map;

/**
 */

public class InMemoryGraphTest extends HugeGraphTest {

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        return config.getConfiguration();
    }

    @Override
    public void clopen(Object... settings) {
        if (settings!=null && settings.length>0) {
            if (graph!=null && graph.isOpen()) {
                Preconditions.checkArgument(!graph.vertices().hasNext() &&
                    !graph.edges().hasNext(),"Graph cannot be re-initialized for InMemory since that would delete all data");
                graph.close();
            }
            Map<TestConfigOption,Object> options = validateConfigOptions(settings);
            ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
            config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
            for (Map.Entry<TestConfigOption,Object> option : options.entrySet()) {
                config.set(option.getKey().option, option.getValue(), option.getKey().umbrella);
            }
            open(config.getConfiguration());
        }
        newTx();
    }

    @Test
    public void testLocalGraphConfiguration() {}

    @Test
    public void testMaskableGraphConfig() {}

    @Test
    public void testGlobalGraphConfig() {}

    @Test
    public void testGlobalOfflineGraphConfig() {}

    @Test
    public void testFixedGraphConfig() {}

    @Override
    public void testManagedOptionMasking() {}

    @Override
    public void testTransactionConfiguration() {}

    @Override
    public void testTinkerPopOptimizationStrategies() {}

    @Override
    public void testDataTypes() {}

    @Override
    public void testForceIndexUsage() {}

    @Override
    public void testAutomaticTypeCreation() {}

    @Override
    public void simpleLogTest() {}

    @Override
    public void simpleLogTestWithFailure() {}

    @Override
    public void testIndexUpdatesWithReindexAndRemove() {}

    @Override
    public void testIndexUpdateSyncWithMultipleInstances() {}

    @Override
    protected boolean isLockingOptimistic() {
        return true;
    }

}
