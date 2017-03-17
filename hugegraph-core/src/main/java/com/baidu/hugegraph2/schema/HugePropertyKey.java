package com.baidu.hugegraph2.schema;

import com.baidu.hugegraph2.schema.base.structure.PropertyKey;
import org.omg.CORBA.Object;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugePropertyKey implements PropertyKey {

    private Class<?> dataType;

    public HugePropertyKey(){
        dataType =Object.class;
    }
    @Override
    public Class<?> dataType() {
        return dataType;
    }

    @Override
    public void setDataType(Class<?> dataType) {

        this.dataType = dataType;
    }

    @Override
    public String name() {
        return null;
    }
}
