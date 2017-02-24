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

package com.baidu.hugegraph.diskstorage.cassandra.embedded;

import static org.junit.Assert.assertTrue;

import com.baidu.hugegraph.diskstorage.BackendException;
import com.baidu.hugegraph.diskstorage.configuration.Configuration;
import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.baidu.hugegraph.CassandraStorageSetup;
import com.baidu.hugegraph.diskstorage.cassandra.AbstractCassandraStoreTest;
import com.baidu.hugegraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.StoreFeatures;
import com.baidu.hugegraph.testcategory.OrderedKeyStoreTests;

public class EmbeddedStoreTest extends AbstractCassandraStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public ModifiableConfiguration getBaseStorageConfiguration() {
        return CassandraStorageSetup.getEmbeddedConfiguration(getClass().getSimpleName());
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager(Configuration c) throws BackendException {
        return new CassandraEmbeddedStoreManager(c);
    }

    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testConfiguration() {
        StoreFeatures features = manager.getFeatures();
        assertTrue(features.isKeyOrdered());
        assertTrue(features.hasLocalKeyPartition());
    }
}
