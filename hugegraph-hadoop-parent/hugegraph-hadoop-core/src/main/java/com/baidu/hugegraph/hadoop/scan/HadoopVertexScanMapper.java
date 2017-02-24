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

package com.baidu.hugegraph.hadoop.scan;

import com.baidu.hugegraph.core.HugeGraphFactory;
import com.baidu.hugegraph.core.HugeGraph;
import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.graphdb.olap.VertexJobConverter;
import com.baidu.hugegraph.graphdb.olap.VertexScanJob;
import com.baidu.hugegraph.hadoop.config.ModifiableHadoopConfiguration;
import com.baidu.hugegraph.hadoop.config.HugeGraphHadoopConfiguration;
import java.io.IOException;

import static com.baidu.hugegraph.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

public class HadoopVertexScanMapper extends HadoopScanMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /* Don't call super implementation super.setup(context); */
        org.apache.hadoop.conf.Configuration hadoopConf = DEFAULT_COMPAT.getContextConfiguration(context);
        ModifiableHadoopConfiguration scanConf =
                ModifiableHadoopConfiguration.of(HugeGraphHadoopConfiguration.MAPRED_NS, hadoopConf);
        VertexScanJob vertexScan = getVertexScanJob(scanConf);
        ModifiableConfiguration graphConf = getHugeGraphConfiguration(context);
        HugeGraph graph = HugeGraphFactory.open(graphConf);
        job = VertexJobConverter.convert(graph, vertexScan);
        metrics = new HadoopContextScanMetrics(context);
        finishSetup(scanConf, graphConf);
    }

    private VertexScanJob getVertexScanJob(ModifiableHadoopConfiguration conf) {
        String jobClass = conf.get(HugeGraphHadoopConfiguration.SCAN_JOB_CLASS);

        try {
            return (VertexScanJob) Class.forName(jobClass).newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
