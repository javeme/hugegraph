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

package com.baidu.hugegraph.hadoop.formats.util;

import com.baidu.hugegraph.diskstorage.Entry;
import com.baidu.hugegraph.diskstorage.StaticBuffer;
import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.hadoop.config.ModifiableHadoopConfiguration;
import com.baidu.hugegraph.hadoop.config.HugeGraphHadoopConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPoolsConfigurable;

public abstract class AbstractBinaryInputFormat extends InputFormat<StaticBuffer, Iterable<Entry>> implements HadoopPoolsConfigurable {

    protected Configuration hadoopConf;
    protected ModifiableHadoopConfiguration mrConf;
    protected ModifiableConfiguration hugegraphConf;

    @Override
    public void setConf(final Configuration config) {
        HadoopPoolsConfigurable.super.setConf(config);
        this.mrConf = ModifiableHadoopConfiguration.of(HugeGraphHadoopConfiguration.MAPRED_NS, config);
        this.hadoopConf = config;
        this.hugegraphConf = mrConf.getHugeGraphConf();
    }

    @Override
    public Configuration getConf() {
        return hadoopConf;
    }
}
