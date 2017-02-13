/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Created by zhangsuochao on 17/2/6.
 */
public class HugeVertexProperty<V> implements VertexProperty<V> {

    @Override
    public String key() {
        return null;
    }

    @Override
    public V value() throws NoSuchElementException {
        return null;
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public Vertex element() {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public Object id() {
        return null;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        return null;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return null;
    }
}
