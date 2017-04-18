package com.baidu.hugegraph.type.define;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Created by jishilei on 17/3/18.
 */

public enum Cardinality {

    /**
     * Only a single value may be associated with the given key.
     */
    SINGLE(1),

    /**
     * Multiple values and duplicate values may be associated with the given key.
     */
    LIST(2),

    /**
     * Multiple but distinct values may be associated with the given key.
     */
    SET(3);

    // HugeKeys define
    private byte code = 0;

    private Cardinality(int code) {
        assert code < 256;
        this.code = (byte) code;
    }

    public byte code() {
        return this.code;
    }

    public VertexProperty.Cardinality convert() {
        switch (this) {
            case SINGLE:
                return VertexProperty.Cardinality.single;
            case LIST:
                return VertexProperty.Cardinality.list;
            case SET:
                return VertexProperty.Cardinality.set;
            default:
                throw new AssertionError("Unrecognized cardinality: " + this);
        }
    }

    public static Cardinality convert(VertexProperty.Cardinality cardinality) {
        switch (cardinality) {
            case single:
                return SINGLE;
            case list:
                return LIST;
            case set:
                return SET;
            default:
                throw new AssertionError("Unrecognized cardinality: " + cardinality);
        }
    }

    public String schema() {
        // enum object -> string -> lowercase
        return this.toString().toLowerCase();
    }
}