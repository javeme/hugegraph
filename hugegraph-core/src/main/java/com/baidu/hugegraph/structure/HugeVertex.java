/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.utils.HugeGraphUtils;

/**
 * Created by zhangsuochao on 17/2/6.
 */
public class HugeVertex extends HugeElement implements Vertex {


    public HugeVertex(final HugeGraph graph,final Object id,final String label){
        super(graph,id,label);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = HugeGraphUtils.generateIdIfNeeded(null);
        HugeEdge edge = new HugeEdge(this.graph(),idValue,label,inVertex,this);
        Long currentTime = System.currentTimeMillis();
        edge.setCreatedAt(currentTime);
        edge.setUpdatedAt(currentTime);
        edge.setProperties(keyValues);
        ((HugeGraph)this.graph()).getEdgeService().addEdge(edge);
        return edge;
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value,
                                          Object... keyValues) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return null;
    }


    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        return null;
    }

}
