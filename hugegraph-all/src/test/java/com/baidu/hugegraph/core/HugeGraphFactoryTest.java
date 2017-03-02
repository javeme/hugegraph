/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.core;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Created by houzhizhen on 17-3-1.
 */
public class HugeGraphFactoryTest {
    public static void main(String[] args) throws ConfigurationException {

        Configuration conf = new PropertiesConfiguration("/home/houzhizhen/git/graphdb/baidu/xbu-data/hugegraph/dist/" +
                "conf/gremlin-server/hugegraph-cassandra-es-server.properties");
        HugeGraph graph = HugeGraphFactory.open(conf);
    }

}
