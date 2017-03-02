/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.pkgtest.com.baidu.hugegraph.core;

import com.baidu.hugegraph.core.HugeGraph;
import com.baidu.hugegraph.core.HugeGraphFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import com.baidu.hugegraph.diskstorage.cassandra.thrift.CassandraThriftStoreManager;
import com.baidu.hugegraph.diskstorage.hbase.HBaseStoreManager;
/**
 * Created by houzhizhen on 17-3-1.
 */
public class HugeGraphFactoryTest {
    public static void main(String[] args) throws ConfigurationException {
        System.out.println("start HugeGraphFactoryTest");
        Configuration conf = new PropertiesConfiguration("/home/houzhizhen/git/graphdb/baidu/xbu-data/hugegraph/dist/"
                +  "conf/gremlin-server/hugegraph-cassandra-es-server.properties");
        HugeGraph graph = HugeGraphFactory.open(conf);
    }

}
