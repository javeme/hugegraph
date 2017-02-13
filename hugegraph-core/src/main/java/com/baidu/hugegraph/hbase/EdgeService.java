/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeGraph;
import com.baidu.hugegraph.utils.Constants;
import com.baidu.hugegraph.utils.HugeGraphUtils;
import com.baidu.hugegraph.utils.ValueUtils;

/**
 * Created by zhangsuochao on 17/2/7.
 */
public class EdgeService extends BaseService {

    private static final Logger logger = LoggerFactory.getLogger(EdgeService.class);

    public EdgeService(HugeGraph graph) {
        super(graph);
    }

    @Override
    public void initTable(Connection connection) {
        try {
            this.table = connection.getTable(TableName.valueOf(Constants.EDGES));
        }catch (IOException e){
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    public void addEdge(HugeEdge edge){
        Put put = constructInsertion(edge);
        try {
            this.table.put(put);
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    /**
     *
     * @param id
     * @return
     */
    public Edge findEdge(Object id){
        Get get = new Get(ValueUtils.serializeWithSalt(id));
        try{
            Result result = this.table.get(get);
            return (HugeEdge)HugeGraphUtils.parseResult(HugeElement.ElementType.EDGE,result, graph);
        }catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    private Put constructInsertion(HugeEdge edge){
        final String label = edge.label() != null ? edge.label() : Edge.DEFAULT_LABEL;
        Put put = new Put(ValueUtils.serializeWithSalt(edge.id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.TO_BYTES,
                ValueUtils.serialize(edge.inVertex().id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.FROM_BYTES,
                ValueUtils.serialize(edge.outVertex().id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.LABEL_BYTES,
                ValueUtils.serialize(label));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                ValueUtils.serialize((edge.getCreatedAt())));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                ValueUtils.serialize((edge.getUpdatedAt())));
        for(String key:edge.getProperties().keySet()){
            byte[] keyBytes = Bytes.toBytes(key);
            byte[] valueBytes = ValueUtils.serialize(edge.getProperties().get(key));
            put.addColumn(Constants.DEFAULT_FAMILY_BYTES,keyBytes,valueBytes);
        }

        return put;
    }

}
