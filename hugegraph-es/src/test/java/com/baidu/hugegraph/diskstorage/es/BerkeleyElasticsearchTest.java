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

package com.baidu.hugegraph.diskstorage.es;

import com.baidu.hugegraph.StorageSetup;
import com.baidu.hugegraph.core.HugeGraph;
import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.WriteConfiguration;
import com.baidu.hugegraph.example.GraphOfTheGodsFactory;
import com.baidu.hugegraph.graphdb.HugeGraphIndexTest;
import com.baidu.hugegraph.util.system.IOUtils;
import org.junit.Test;

import java.io.File;

import static com.baidu.hugegraph.diskstorage.es.ElasticSearchIndex.*;
import static com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static com.baidu.hugegraph.BerkeleyStorageSetup.getBerkeleyJEConfiguration;

/**
 */

public class BerkeleyElasticsearchTest extends HugeGraphIndexTest {

    public BerkeleyElasticsearchTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = getBerkeleyJEConfiguration();
        //Add index
        config.set(INDEX_BACKEND,"elasticsearch",INDEX);
        config.set(LOCAL_MODE,true,INDEX);
        config.set(CLIENT_ONLY,false,INDEX);
        config.set(INDEX_DIRECTORY, StorageSetup.getHomeDir("es"), INDEX);
        return config.getConfiguration();

    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public boolean supportsWildcardQuery() {
        return true;
    }

    @Override
    protected boolean supportsCollections() {
        return true;
    }

    /**
     * Test {@link com.baidu.hugegraph.example.GraphOfTheGodsFactory#create(String)}.
     */
    @Test
    public void testGraphOfTheGodsFactoryCreate() {
        File bdbtmp = new File("target/gotgfactory");
        IOUtils.deleteDirectory(bdbtmp, true);

        HugeGraph gotg = GraphOfTheGodsFactory.create(bdbtmp.getPath());
        HugeGraphIndexTest.assertGraphOfTheGods(gotg);
        gotg.close();
    }
}
