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

package com.baidu.hugegraph.pkgtest;

import com.baidu.hugegraph.diskstorage.es.ElasticsearchRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.CassandraStorageSetup;

public class CassandraThriftESAssemblyIT extends AbstractHugeGraphAssemblyIT {

    public static final String ES_HOME = "../../hugegraph-es";

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @BeforeClass
    public static void startES() {
        new ElasticsearchRunner(ES_HOME).start();
    }

    @Test
    public void testCassandraGettingStarted() throws Exception {
        testGettingStartedGremlinSession("conf/hugegraph-cassandra-es.properties", "cassandrathrift");
    }

    @AfterClass
    public static void stopES() {
        new ElasticsearchRunner(ES_HOME).stop();
    }
}
