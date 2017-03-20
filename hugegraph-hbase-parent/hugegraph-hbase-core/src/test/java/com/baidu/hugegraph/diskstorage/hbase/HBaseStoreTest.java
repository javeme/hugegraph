// Copyright 2017 hugegraph Authors
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

package com.baidu.hugegraph.diskstorage.hbase;

import com.baidu.hugegraph.HBaseStorageSetup;
import com.baidu.hugegraph.diskstorage.BackendException;
import com.baidu.hugegraph.diskstorage.KeyColumnValueStoreTest;
import com.baidu.hugegraph.diskstorage.configuration.BasicConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.WriteConfiguration;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class HBaseStoreTest extends KeyColumnValueStoreTest {

    @BeforeClass
    public static void startHBase() throws IOException, BackendException {
        HBaseStorageSetup.startHBase();
    }

    @AfterClass
    public static void stopHBase() {
        // Workaround for https://issues.apache.org/jira/browse/HBASE-10312
        if (VersionInfo.getVersion().startsWith("0.96"))
            HBaseStorageSetup.killIfRunning();
    }

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        WriteConfiguration config = HBaseStorageSetup.getHBaseGraphConfiguration();
        return new HBaseStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,config, BasicConfiguration.Restriction.NONE));
    }

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }
}