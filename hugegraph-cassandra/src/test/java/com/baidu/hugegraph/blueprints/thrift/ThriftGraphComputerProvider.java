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

package com.baidu.hugegraph.blueprints.thrift;

import com.baidu.hugegraph.CassandraStorageSetup;
import com.baidu.hugegraph.blueprints.AbstractHugeGraphComputerProvider;
import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.graphdb.olap.computer.FulgoraGraphComputer;
import org.apache.tinkerpop.gremlin.GraphProvider;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@GraphProvider.Descriptor(computer = FulgoraGraphComputer.class)
public class ThriftGraphComputerProvider extends AbstractHugeGraphComputerProvider {

    @Override
    public ModifiableConfiguration getHugeGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        CassandraStorageSetup.startCleanEmbedded();
        ModifiableConfiguration config = super.getHugeGraphConfiguration(graphName, test, testMethodName);
        config.setAll(CassandraStorageSetup.getCassandraThriftConfiguration(graphName).getAll());
        return config;
    }

}
