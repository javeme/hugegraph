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

package com.baidu.hugegraph.diskstorage.inmemory;

import com.baidu.hugegraph.diskstorage.BackendException;
import com.baidu.hugegraph.diskstorage.LockKeyColumnValueStoreTest;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager(int id) throws BackendException {
        return new InMemoryStoreManager();
    }

    @Override
    public void testRemoteLockContention() {
        // Does not apply to non-persisting in-memory store
    }

}
