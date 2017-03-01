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

package com.baidu.hugegraph.diskstorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.baidu.hugegraph.diskstorage.common.DistributedStoreManager;
import com.baidu.hugegraph.diskstorage.common.DistributedStoreManager.Deployment;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.KeyRange;
import com.baidu.hugegraph.testcategory.OrderedKeyStoreTests;

public abstract class DistributedStoreManagerTest<T extends DistributedStoreManager> {
    
    protected T manager;
    protected KeyColumnValueStore store;
    
    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testGetDeployment() {
        assertEquals(Deployment.LOCAL, manager.getDeployment());
    }
    
    @Test
    @Category({ OrderedKeyStoreTests.class })
    public void testGetLocalKeyPartition() throws BackendException {
        List<KeyRange> local = manager.getLocalKeyPartition();
        assertNotNull(local);
        assertEquals(1, local.size());
        assertNotNull(local.get(0).getStart());
        assertNotNull(local.get(0).getEnd());
    }
}
