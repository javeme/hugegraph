package com.baidu.hugegraph2.schema;

import com.baidu.hugegraph2.example.ExampleGraphFactory;
import com.baidu.hugegraph2.schema.base.maker.EdgeLabelMaker;
import com.baidu.hugegraph2.schema.base.maker.PropertyKeyMaker;
import com.baidu.hugegraph2.schema.base.maker.SchemaManager;
import com.baidu.hugegraph2.schema.base.maker.VertexLabelMaker;
import com.baidu.hugegraph2.schema.base.structure.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeSchemaManager implements SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(HugeSchemaManager.class);
    private  PropertyKeyMaker propertyKeyMaker;

    private Map<String,PropertyKeyMaker> propertyKeyMakerMap;
    public HugeSchemaManager(){

        propertyKeyMakerMap = new HashMap<String,PropertyKeyMaker>();
    }
    @Override
    public PropertyKeyMaker propertyKey(String name) {
        propertyKeyMaker = new HugePropertyKeyMaker(name);
        propertyKeyMakerMap.put(name,propertyKeyMaker);
        return propertyKeyMaker;
    }

    @Override
    public VertexLabelMaker vertexLabel(String name) {
        return null;
    }

    @Override
    public EdgeLabelMaker edgeLabel(String name) {
        return null;
    }

    @Override
    public void desc() {

        for(String k:propertyKeyMakerMap.keySet()){
            HugePropertyKeyMaker maker = (HugePropertyKeyMaker)propertyKeyMakerMap.get(k);
            logger.info(k + " = " + maker.getPropertyKey().dataType());
        }

    }
}
