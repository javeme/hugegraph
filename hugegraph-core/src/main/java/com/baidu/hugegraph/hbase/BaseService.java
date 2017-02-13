/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import com.baidu.hugegraph.structure.HugeGraph;
import com.baidu.hugegraph.structure.HugeGraphConfiguration;
import com.baidu.hugegraph.utils.Constants;
import com.baidu.hugegraph.utils.ValueUtils;

/**
 * Created by zhangsuochao on 17/2/7.
 */
public abstract class BaseService {
    protected final HugeGraph graph;
    protected Table table;
    protected Connection connection;
    public BaseService(HugeGraph graph){
        this.graph = graph;
        init();
        initTable(this.connection);
    }

    public abstract void initTable(Connection connection);

    /**
     * Init hbase connection
     */
    protected void init(){
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(HugeGraphConfiguration.Keys.ZOOKEEPER_QUORUM,graph.configuration().getString(HugeGraphConfiguration.Keys.ZOOKEEPER_QUORUM));
        hbaseConf.set(HugeGraphConfiguration.Keys.ZOOKEEPER_CLIENTPORT,graph.configuration().getString
                (HugeGraphConfiguration.Keys.ZOOKEEPER_CLIENTPORT));
        try {
            this.connection = ConnectionFactory.createConnection(hbaseConf);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    protected Scan getPropertyScan(String label) {
        Scan scan = new Scan();
        SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                Constants.LABEL_BYTES, CompareFilter.CompareOp.EQUAL, new BinaryComparator(ValueUtils.serialize(label)));
        valueFilter.setFilterIfMissing(true);
        scan.setFilter(valueFilter);
        return scan;
    }

    protected Scan getPropertyScan(String label, byte[] key, byte[] val) {
        Scan scan = new Scan();
        SingleColumnValueFilter labelFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                Constants.LABEL_BYTES, CompareFilter.CompareOp.EQUAL, new BinaryComparator(ValueUtils.serialize(label)));
        labelFilter.setFilterIfMissing(true);
        SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                key, CompareFilter.CompareOp.EQUAL, new BinaryComparator(val));
        valueFilter.setFilterIfMissing(true);
        FilterList filterList = new FilterList(labelFilter, valueFilter);
        scan.setFilter(filterList);
        return scan;
    }

}
