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
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import com.baidu.hugegraph.olap.OLAPTest;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryOLAPTest extends OLAPTest {

    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        return config.getConfiguration();
    }

    @Override
    public void clopen(Object...settings) {
        Preconditions.checkArgument(settings == null || settings.length == 0);
        newTx();
    }

}
