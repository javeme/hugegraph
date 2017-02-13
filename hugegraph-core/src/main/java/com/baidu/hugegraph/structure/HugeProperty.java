/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * Created by zhangsuochao on 17/2/6.
 */
public class HugeProperty<V> implements Property<V> {
    @Override
    public void ifPresent(Consumer<? super V> consumer) {

    }

    @Override
    public V orElse(V otherValue) {
        return null;
    }

    @Override
    public V orElseGet(Supplier<? extends V> valueSupplier) {
        return null;
    }

    @Override
    public <E extends Throwable> V orElseThrow(Supplier<? extends E> exceptionSupplier) throws E {
        return null;
    }

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
    public Element element() {
        return null;
    }

    @Override
    public void remove() {

    }
}
