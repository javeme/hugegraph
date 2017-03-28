package com.baidu.hugegraph2.type.define;


/**
 * Created by jishilei on 17/3/18.
 */
public enum Multiplicity {
    /**
     * The given edge label specifies a multi-graph, meaning that the multiplicity is not constrained and that
     * there may be multiple edges of this label between any given pair of vertices.
     */
    MANY2MANY,

    /**
     * There can only be a single in-edge of this label for a given vertex but multiple out-edges (i.e. in-unique)
     */
    ONE2MANY,

    /**
     * There can only be a single out-edge of this label for a given vertex but multiple in-edges (i.e. out-unique)
     */
    MANY2ONE,

    /**
     * There can be only a single in and out-edge of this label for a given vertex (i.e. unique in both directions).
     */
    ONE2ONE;


    public String schema() {
        String shema = "link";
        switch (this) {
            case ONE2ONE:
                shema += "One2One";
                break;
            case ONE2MANY:
                shema += "One2Many";
                break;
            case MANY2ONE:
                shema += "Many2One";
                break;
            case MANY2MANY:
                shema += "Many2Many";
                break;
        }
        return shema;
    }
}
