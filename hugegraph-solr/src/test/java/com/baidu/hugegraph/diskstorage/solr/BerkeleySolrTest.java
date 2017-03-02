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

package com.baidu.hugegraph.diskstorage.solr;

import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.WriteConfiguration;
import static com.baidu.hugegraph.BerkeleyStorageSetup.getBerkeleyJEConfiguration;
import static com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 */

public class BerkeleySolrTest extends SolrHugeGraphIndexTest {

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = getBerkeleyJEConfiguration();
        //Add index
        config.set(INDEX_BACKEND,"solr",INDEX);
        config.set(SolrIndex.ZOOKEEPER_URL, SolrRunner.getMiniCluster().getZkServer().getZkAddress(), INDEX);
        config.set(SolrIndex.WAIT_SEARCHER, true, INDEX);
        //TODO: set SOLR specific config options
        return config.getConfiguration();
    }

    @Override
    public boolean supportsWildcardQuery() {
        return false;
    }
}
