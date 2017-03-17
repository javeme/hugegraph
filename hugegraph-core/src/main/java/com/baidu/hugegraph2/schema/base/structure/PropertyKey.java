package com.baidu.hugegraph2.schema.base.structure;

import com.baidu.hugegraph2.schema.base.Namifiable;
import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * Created by jishilei on 17/3/17.
 */
public interface PropertyKey extends SchemaType{
    /**
     * Returns the data type for this property key.
     * The values of all properties of this type must be an instance of this data type.
     *
     * @return Data type for this property key.
     */
    public Class<?> dataType();

    public void setDataType(Class<?> dataType);


}
