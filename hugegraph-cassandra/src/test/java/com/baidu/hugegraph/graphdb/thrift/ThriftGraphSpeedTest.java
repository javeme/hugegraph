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

import com.baidu.hugegraph.diskstorage.BackendException;
import com.baidu.hugegraph.graphdb.SpeedTestSchema;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.CassandraStorageSetup;
import com.baidu.hugegraph.core.HugeGraphFactory;
import com.baidu.hugegraph.graphdb.HugeGraphSpeedTest;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;
import com.baidu.hugegraph.testcategory.PerformanceTests;

@Category({ PerformanceTests.class })
public class ThriftGraphSpeedTest extends HugeGraphSpeedTest {

    private static StandardHugeGraph graph;
    private static SpeedTestSchema schema;

    private static final Logger log = LoggerFactory.getLogger(ThriftGraphSpeedTest.class);

    public ThriftGraphSpeedTest() throws BackendException {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(ThriftGraphSpeedTest.class.getSimpleName()));
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    protected StandardHugeGraph getGraph() throws BackendException {

        if (null == graph) {
            GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(conf);
            graphconfig.getBackend().clearStorage();
            log.debug("Cleared backend storage");
            graph = (StandardHugeGraph) HugeGraphFactory.open(conf);
            initializeGraph(graph);
        }
        return graph;
    }

    @Override
    protected SpeedTestSchema getSchema() {
        if (null == schema) {
            schema = SpeedTestSchema.get();
        }
        return schema;
    }
}
