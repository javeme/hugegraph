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

package com.baidu.hugegraph.hadoop;

import com.baidu.hugegraph.diskstorage.configuration.BasicConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.ConfigElement;
import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.diskstorage.configuration.backend.CommonsConfiguration;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import com.baidu.hugegraph.graphdb.olap.job.IndexRemoveJob;
import com.baidu.hugegraph.graphdb.olap.job.IndexRepairJob;
import com.baidu.hugegraph.hadoop.config.HugeGraphHadoopConfiguration;
import com.baidu.hugegraph.hadoop.scan.CassandraHadoopScanRunner;
import com.baidu.hugegraph.hadoop.scan.HBaseHadoopScanRunner;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import com.baidu.hugegraph.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class MapReduceIndexJobs {

    private static final Logger log =
            LoggerFactory.getLogger(MapReduceIndexJobs.class);

    public static ScanMetrics cassandraRepair(String hugegraphPropertiesPath, String indexName, String relationType, String partitionerName)
            throws InterruptedException, IOException, ClassNotFoundException {
        Properties p = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(hugegraphPropertiesPath);
            p.load(fis);
            return cassandraRepair(p, indexName, relationType, partitionerName);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    public static ScanMetrics cassandraRepair(Properties hugegraphProperties, String indexName, String relationType,
                                              String partitionerName)
            throws InterruptedException, IOException, ClassNotFoundException {
        return cassandraRepair(hugegraphProperties, indexName, relationType, partitionerName, new Configuration());
    }

    public static ScanMetrics cassandraRepair(Properties hugegraphProperties, String indexName, String relationType,
                                              String partitionerName, Configuration hadoopBaseConf)
            throws InterruptedException, IOException, ClassNotFoundException {
        IndexRepairJob job = new IndexRepairJob();
        CassandraHadoopScanRunner cr = new CassandraHadoopScanRunner(job);
        ModifiableConfiguration mc = getIndexJobConf(indexName, relationType);
        copyPropertiesToInputAndOutputConf(hadoopBaseConf, hugegraphProperties);
        cr.partitionerOverride(partitionerName);
        cr.scanJobConf(mc);
        cr.scanJobConfRoot(GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        cr.baseHadoopConf(hadoopBaseConf);
        return cr.run();
    }


    public static ScanMetrics cassandraRemove(String hugegraphPropertiesPath, String indexName, String relationType, String partitionerName)
            throws InterruptedException, IOException, ClassNotFoundException {
        Properties p = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(hugegraphPropertiesPath);
            p.load(fis);
            return cassandraRemove(p, indexName, relationType, partitionerName);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    public static ScanMetrics cassandraRemove(Properties hugegraphProperties, String indexName, String relationType,
                                              String partitionerName)
            throws InterruptedException, IOException, ClassNotFoundException {
        return cassandraRemove(hugegraphProperties, indexName, relationType, partitionerName, new Configuration());
    }

    public static ScanMetrics cassandraRemove(Properties hugegraphProperties, String indexName, String relationType,
                                              String partitionerName, Configuration hadoopBaseConf)
            throws InterruptedException, IOException, ClassNotFoundException {
        IndexRemoveJob job = new IndexRemoveJob();
        CassandraHadoopScanRunner cr = new CassandraHadoopScanRunner(job);
        ModifiableConfiguration mc = getIndexJobConf(indexName, relationType);
        copyPropertiesToInputAndOutputConf(hadoopBaseConf, hugegraphProperties);
        cr.partitionerOverride(partitionerName);
        cr.scanJobConf(mc);
        cr.scanJobConfRoot(GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        cr.baseHadoopConf(hadoopBaseConf);
        return cr.run();
    }

    public static ScanMetrics hbaseRepair(String hugegraphPropertiesPath, String indexName, String relationType)
            throws InterruptedException, IOException, ClassNotFoundException {
        Properties p = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(hugegraphPropertiesPath);
            p.load(fis);
            return hbaseRepair(p, indexName, relationType);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    public static ScanMetrics hbaseRepair(Properties hugegraphProperties, String indexName, String relationType)
            throws InterruptedException, IOException, ClassNotFoundException {
        return hbaseRepair(hugegraphProperties, indexName, relationType, new Configuration());
    }

    public static ScanMetrics hbaseRepair(Properties hugegraphProperties, String indexName, String relationType,
                                          Configuration hadoopBaseConf)
            throws InterruptedException, IOException, ClassNotFoundException {
        IndexRepairJob job = new IndexRepairJob();
        HBaseHadoopScanRunner cr = new HBaseHadoopScanRunner(job);
        ModifiableConfiguration mc = getIndexJobConf(indexName, relationType);
        copyPropertiesToInputAndOutputConf(hadoopBaseConf, hugegraphProperties);
        cr.scanJobConf(mc);
        cr.scanJobConfRoot(GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        cr.baseHadoopConf(hadoopBaseConf);
        return cr.run();
    }

    public static ScanMetrics hbaseRemove(String hugegraphPropertiesPath, String indexName, String relationType)
            throws InterruptedException, IOException, ClassNotFoundException {
        Properties p = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(hugegraphPropertiesPath);
            p.load(fis);
            return hbaseRemove(p, indexName, relationType);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    public static ScanMetrics hbaseRemove(Properties hugegraphProperties, String indexName, String relationType)
            throws InterruptedException, IOException, ClassNotFoundException {
        return hbaseRemove(hugegraphProperties, indexName, relationType, new Configuration());
    }

    public static ScanMetrics hbaseRemove(Properties hugegraphProperties, String indexName, String relationType,
                                          Configuration hadoopBaseConf)
            throws InterruptedException, IOException, ClassNotFoundException {
        IndexRemoveJob job = new IndexRemoveJob();
        HBaseHadoopScanRunner cr = new HBaseHadoopScanRunner(job);
        ModifiableConfiguration mc = getIndexJobConf(indexName, relationType);
        copyPropertiesToInputAndOutputConf(hadoopBaseConf, hugegraphProperties);
        cr.scanJobConf(mc);
        cr.scanJobConfRoot(GraphDatabaseConfiguration.class.getName() + "#JOB_NS");
        cr.baseHadoopConf(hadoopBaseConf);
        return cr.run();
    }

    private static ModifiableConfiguration getIndexJobConf(String indexName, String relationType) {
        ModifiableConfiguration mc = new ModifiableConfiguration(GraphDatabaseConfiguration.JOB_NS,
                new CommonsConfiguration(new BaseConfiguration()), BasicConfiguration.Restriction.NONE);
        mc.set(com.baidu.hugegraph.graphdb.olap.job.IndexUpdateJob.INDEX_NAME, indexName);
        mc.set(com.baidu.hugegraph.graphdb.olap.job.IndexUpdateJob.INDEX_RELATION_TYPE, relationType);
        mc.set(GraphDatabaseConfiguration.JOB_START_TIME, System.currentTimeMillis());
        return mc;
    }

    private static void copyPropertiesToInputAndOutputConf(Configuration sink, Properties source) {
        final String prefix = ConfigElement.getPath(HugeGraphHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + ".";
        for (Map.Entry<Object, Object> e : source.entrySet()) {
            String k;
            String v = e.getValue().toString();
            k = prefix + e.getKey().toString();
            sink.set(k, v);
            log.info("Set {}={}", k, v);
        }
    }
}
