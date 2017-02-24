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

package com.baidu.hugegraph.graphdb.thrift;

import com.baidu.hugegraph.CassandraStorageSetup;
import com.baidu.hugegraph.diskstorage.BackendException;
import com.baidu.hugegraph.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.baidu.hugegraph.diskstorage.configuration.BasicConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.WriteConfiguration;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.baidu.hugegraph.graphdb.HugeGraphIterativeBenchmark;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.BeforeClass;

public class ThriftGraphIterativeTest extends HugeGraphIterativeBenchmark {

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getCassandraThriftGraphConfiguration(getClass().getSimpleName());
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return new CassandraThriftStoreManager(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                getConfiguration(), BasicConfiguration.Restriction.NONE));
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }
}
