package com.baidu.hugegraph2;

import com.baidu.hugegraph.core.*;
import com.google.common.base.Preconditions;

import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * Created by jishilei on 17/3/18.
 */
public enum Multiplicity {
    /**
     * The given edge label specifies a multi-graph, meaning that the multiplicity is not constrained and that
     * there may be multiple edges of this label between any given pair of vertices.
     *
     * @link http://en.wikipedia.org/wiki/Multigraph
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
    ONE2ONE

}
