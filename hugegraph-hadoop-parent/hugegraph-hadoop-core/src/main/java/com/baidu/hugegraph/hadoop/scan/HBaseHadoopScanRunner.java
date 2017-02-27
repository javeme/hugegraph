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

package com.baidu.hugegraph.hadoop.scan;

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.diskstorage.configuration.ConfigElement;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.scan.ScanJob;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.baidu.hugegraph.graphdb.olap.VertexScanJob;
import com.baidu.hugegraph.hadoop.config.HugeGraphHadoopConfiguration;
import com.baidu.hugegraph.hadoop.formats.hbase.HBaseBinaryInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// TODO port HBase-Kerberos auth token support
public class HBaseHadoopScanRunner extends AbstractHadoopScanRunner<HBaseHadoopScanRunner> {

    private static final Logger log = LoggerFactory.getLogger(HBaseHadoopScanRunner.class);

    public HBaseHadoopScanRunner(ScanJob scanJob) {
        super(scanJob);
    }

    public HBaseHadoopScanRunner(VertexScanJob vertexScanJob) {
        super(vertexScanJob);
    }

    @Override
    protected HBaseHadoopScanRunner self() {
        return this;
    }

    public ScanMetrics run() throws InterruptedException, IOException, ClassNotFoundException {

        org.apache.hadoop.conf.Configuration hadoopConf = null != baseHadoopConf ?
                baseHadoopConf : new org.apache.hadoop.conf.Configuration();

        if (null != hugegraphConf) {
            String prefix = ConfigElement.getPath(HugeGraphHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + ".";
            for (String k : hugegraphConf.getKeys("")) {
                hadoopConf.set(prefix + k, hugegraphConf.get(k, Object.class).toString());
                log.debug("Set: {}={}", prefix + k, hugegraphConf.get(k, Object.class).toString());
            }
        }
        Preconditions.checkNotNull(hadoopConf);

        if (null != scanJob) {
            return HadoopScanRunner.runScanJob(scanJob, scanJobConf, scanJobConfRoot, hadoopConf, HBaseBinaryInputFormat.class);
        } else {
            return HadoopScanRunner.runVertexScanJob(vertexScanJob, scanJobConf, scanJobConfRoot, hadoopConf, HBaseBinaryInputFormat.class);
        }
    }
}
