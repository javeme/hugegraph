/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeGraph;
import com.baidu.hugegraph.structure.HugeVertex;

/**
 * Created by zhangsuochao on 17/2/8.
 */
public class HugeGraphUtils {
    public static Object generateIdIfNeeded(Object id) {
        if (id == null) {
            id = UUID.randomUUID().toString();
        } else if (id instanceof Integer) {
            id = ((Integer) id).longValue();
        } else if (id instanceof Number) {
            id = ((Number) id).longValue();
        }
        return id;
    }

    /**
     *
     * @param scanner
     * @param graph
     * @return
     * @throws IOException
     */
    public static Iterator<Vertex> parseScanner(ResultScanner scanner, HugeGraph graph) throws IOException {
        Result result;
        Vertex vertex;
        List<Vertex> lst= new ArrayList<>();
        while ((result=scanner.next()) != null){
            vertex = (HugeVertex)parseResult(HugeElement.ElementType.VERTEX, result, graph);
            if(vertex != null){
                lst.add(vertex);
            }
        }
        return lst.iterator();
    }


    /**
     *
     * @param result
     * @param graph
     * @return
     */
    public static HugeElement parseResult(HugeElement.ElementType elementType, Result result, HugeGraph graph){
        if(result.isEmpty()){
            return null;
        }

        Object id = ValueUtils.deserializeWithSalt(result.getRow());
        String label = null;
        Long createdAt = null;
        Long updatedAt = null;
        Map<String, Object> rawProps = new HashMap<>();
        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (!Graph.Hidden.isHidden(key)) {
                rawProps.put(key, ValueUtils.deserialize(CellUtil.cloneValue(cell)));
            } else if (key.equals(Constants.LABEL)) {
                label = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.CREATED_AT)) {
                createdAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.UPDATED_AT)) {
                updatedAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            }
        }

        HugeElement element = null;
        if(HugeElement.ElementType.VERTEX.equals(elementType)){
            element = new HugeVertex(graph,id,label);

        }else if (HugeElement.ElementType.EDGE.equals(elementType)){
            element = new HugeEdge(graph,id,label);
        }

        element.setCreatedAt(createdAt);
        element.setUpdatedAt(updatedAt);
        element.setPropertyMap(rawProps);

        return element;
    }

    public static Map<String, Object> propertiesToMap(Object... keyValues) {
        Map<String, Object> props = new HashMap<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            Object key = keyValues[i];
            if (key.equals(T.id) || key.equals(T.label))
                continue;
            String keyStr = key.toString();
            Object value = keyValues[i + 1];
            ElementHelper.validateProperty(keyStr, value);
            props.put(keyStr, value);
        }
        return props;
    }
}
