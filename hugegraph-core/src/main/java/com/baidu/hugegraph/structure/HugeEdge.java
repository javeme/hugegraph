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
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.utils.HugeGraphUtils;

/**
 * Created by zhangsuochao on 17/2/9.
 */
public class HugeEdge extends HugeElement implements Edge {

    private static final Logger logger = LoggerFactory.getLogger(HugeEdge.class);

    private Vertex inVertex;
    private Vertex outVertex;

    public HugeEdge(final HugeGraph graph,final Object id,final String label){
        super(graph, id, label);

    }

    public HugeEdge(final Graph graph, final Object id, final String label, final Vertex inVertex, final
                    Vertex outVertex){
        super(graph, id, label);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }


    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        return direction == Direction.BOTH
                ? IteratorUtils.of(getVertex(Direction.OUT), getVertex(Direction.IN))
                : IteratorUtils.of(getVertex(direction));
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator< Property<V>> properties(String... propertyKeys) {
        return null;
    }

    public void setProperties(Object... keyValues){
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        this.properties = HugeGraphUtils.propertiesToMap(keyValues);
    }

    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (!Direction.IN.equals(direction) && !Direction.OUT.equals(direction)) {
            throw new IllegalArgumentException("Invalid direction: " + direction);
        }

        return Direction.IN.equals(direction) ? inVertex : outVertex;
    }
}
