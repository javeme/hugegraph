package com.baidu.hugegraph2.schema;

import com.baidu.hugegraph2.schema.base.maker.PropertyKeyMaker;
import com.baidu.hugegraph2.schema.base.structure.PropertyKey;
import com.baidu.hugegraph2.structure.HugeProperty;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugePropertyKeyMaker implements PropertyKeyMaker {

    private PropertyKey propertyKey;
    private String name;


    public HugePropertyKeyMaker(String name){
        propertyKey = new HugePropertyKey();
        this.name = name;
    }


    public PropertyKey getPropertyKey(){
        return propertyKey;
    }



    @Override
    public PropertyKeyMaker Text() {
        this.propertyKey.setDataType(String.class);
        return this;
    }

    @Override
    public PropertyKeyMaker Int() {
        this.propertyKey.setDataType(Integer.class);
        return this;
    }


    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public PropertyKey create() {
        return new HugePropertyKey();
    }
}
