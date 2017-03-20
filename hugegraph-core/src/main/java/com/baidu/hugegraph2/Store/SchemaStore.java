package com.baidu.hugegraph2.store;

import com.baidu.hugegraph2.schema.base.PropertyKey;

import java.util.Map;

/**
 * Created by jishilei on 17/3/18.
 */
public interface SchemaStore {

    public Map<String,PropertyKey> getPropertyKeys();
    public void removePropertyKey(String name);
    public void addPropertyKey(PropertyKey propertyKey);

}
