package com.baidu.hugegraph.type.define;


/**
 * Created by jishilei on 17/3/18.
 */
public enum Multiplicity {
    /**
     * The given edge label specifies a multi-graph, meaning that the multiplicity is not constrained and that
     * there may be multiple edges of this label between any given pair of vertices.
     */
    MANY2MANY(1, "many2many"),

    /**
     * There can only be a single in-edge of this label for a given vertex but multiple out-edges (i.e. in-unique)
     */
    ONE2MANY(2, "one2many"),

    /**
     * There can only be a single out-edge of this label for a given vertex but multiple in-edges (i.e. out-unique)
     */
    MANY2ONE(3, "many2one"),

    /**
     * There can be only a single in and out-edge of this label for a given vertex (i.e. unique in both directions).
     */
    ONE2ONE(4, "one2one");

    private byte code = 0;
    private String name = null;

    private Multiplicity(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

}
