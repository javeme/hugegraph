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

package com.baidu.hugegraph.hadoop.formats.hbase;

import com.baidu.hugegraph.diskstorage.Entry;
import com.baidu.hugegraph.diskstorage.PermanentBackendException;
import com.baidu.hugegraph.diskstorage.StaticBuffer;
import com.baidu.hugegraph.diskstorage.hbase.HBaseStoreManager;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.SliceQuery;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import com.baidu.hugegraph.hadoop.config.HugeGraphHadoopConfiguration;
import com.baidu.hugegraph.hadoop.formats.util.AbstractBinaryInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

public class HBaseBinaryInputFormat extends AbstractBinaryInputFormat {

    private static final Logger log = LoggerFactory.getLogger(HBaseBinaryInputFormat.class);

    private final TableInputFormat tableInputFormat = new TableInputFormat();
    private RecordReader<ImmutableBytesWritable, Result> tableReader;
    private byte[] inputCFBytes;
    private RecordReader<StaticBuffer, Iterable<Entry>> hugegraphRecordReader;

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.tableInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<StaticBuffer, Iterable<Entry>> createRecordReader(final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        tableReader = tableInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        hugegraphRecordReader = new HBaseBinaryRecordReader(tableReader, inputCFBytes);
        return hugegraphRecordReader;
    }

    @Override
    public void setConf(final Configuration config) {
        super.setConf(config);

        // config.set(TableInputFormat.SCAN_COLUMN_FAMILY, Backend.EDGESTORE_NAME);
        config.set(TableInputFormat.INPUT_TABLE, hugegraphConf.get(HBaseStoreManager.HBASE_TABLE));
        // config.set(HConstants.ZOOKEEPER_QUORUM,
        // config.get(JANUSGRAPH_HADOOP_GRAPH_INPUT_JANUSGRAPH_STORAGE_HOSTNAME));
        config.set(HConstants.ZOOKEEPER_QUORUM, hugegraphConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
        // if (basicConf.get(JANUSGRAPH_HADOOP_GRAPH_INPUT_JANUSGRAPH_STORAGE_PORT, null) != null)
        if (hugegraphConf.has(GraphDatabaseConfiguration.STORAGE_PORT))
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT,
                    String.valueOf(hugegraphConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        config.set("autotype", "none");
        log.debug("hbase.security.authentication={}", config.get("hbase.security.authentication"));
        Scan scanner = new Scan();
        String cfName = mrConf.get(HugeGraphHadoopConfiguration.COLUMN_FAMILY_NAME);
        // TODO the space-saving short name mapping leaks from HBaseStoreManager here
        if (hugegraphConf.get(HBaseStoreManager.SHORT_CF_NAMES)) {
            try {
                cfName = HBaseStoreManager.shortenCfName(cfName);
            } catch (PermanentBackendException e) {
                throw new RuntimeException(e);
            }
        }
        scanner.addFamily(cfName.getBytes());
        inputCFBytes = Bytes.toBytes(cfName);

        // scanner.setFilter(getColumnFilter(hugegraphSetup.inputSlice(this.vertexQuery))); // TODO
        // TODO (minor): should we set other options in
        // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Scan.html for optimization?
        Method converter;
        try {
            converter = TableMapReduceUtil.class.getDeclaredMethod("convertScanToString", Scan.class);
            converter.setAccessible(true);
            config.set(TableInputFormat.SCAN, (String) converter.invoke(null, scanner));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.tableInputFormat.setConf(config);
    }

    public RecordReader<ImmutableBytesWritable, Result> getTableReader() {
        return tableReader;
    }

    public byte[] getEdgeStoreFamily() {
        return inputCFBytes;
    }

    private Filter getColumnFilter(SliceQuery query) {
        return null;
        // TODO: return HBaseKeyColumnValueStore.getFilter(hugegraphSetup.inputSlice(inputFilter));
    }

    @Override
    public Configuration getConf() {
        return tableInputFormat.getConf();
    }
}
