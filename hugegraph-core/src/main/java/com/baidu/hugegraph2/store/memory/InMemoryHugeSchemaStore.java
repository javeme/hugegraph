package com.baidu.hugegraph2.store.memory;

import com.baidu.hugegraph2.store.SchemaStore;
import com.baidu.hugegraph2.schema.base.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jishilei on 17/3/18.
 */
public class InMemoryHugeSchemaStore implements SchemaStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryHugeSchemaStore.class);
    private Map<String, PropertyKey> propertyKeyMap;

    public InMemoryHugeSchemaStore(){
        propertyKeyMap = new HashMap<String, PropertyKey>();
    }

    @Override
    public Map<String, PropertyKey> getPropertyKeys() {
        return propertyKeyMap;
    }

    @Override
    public void removePropertyKey(String name) {

        propertyKeyMap.remove(name);
    }

    @Override
    public void addPropertyKey(PropertyKey propertyKey) {
        propertyKeyMap.put(propertyKey.name(),propertyKey);
        logger.info("addPropertyKey : " + propertyKey.toString());
    }
}
