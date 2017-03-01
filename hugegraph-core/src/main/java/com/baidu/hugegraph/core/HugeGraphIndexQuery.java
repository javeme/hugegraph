// Copyright 2017 HugeGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.baidu.hugegraph.core;

import com.baidu.hugegraph.core.schema.Parameter;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * A GraphQuery that queries for graph elements directly against a particular indexing backend and hence allows this
 * query mechanism to exploit the full range of features and functionality of the indexing backend.
 * However, the results returned by this query will not be adjusted to the modifications in a transaction. If there
 * are no changes in a transaction, this won't matter. If there are, the results of this query may not be consistent
 * with the transactional state.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface HugeGraphIndexQuery {

    /**
     * Specifies the maxium number of elements to return
     *
     * @param limit
     * @return
     */
    public HugeGraphIndexQuery limit(int limit);

    /**
     * Specifies the offset of the query. Query results will be retrieved starting at the given offset.
     * @param offset
     * @return
     */
    public HugeGraphIndexQuery offset(int offset);

    /**
     * Adds the given parameter to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     * @param para
     * @return
     */
    public HugeGraphIndexQuery addParameter(Parameter para);

    /**
     * Adds the given parameters to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     * @param paras
     * @return
     */
    public HugeGraphIndexQuery addParameters(Iterable<Parameter> paras);

    /**
     * Adds the given parameters to the list of parameters of this query.
     * Parameters are passed right through to the indexing backend to modify the query behavior.
     * @param paras
     * @return
     */
    public HugeGraphIndexQuery addParameters(Parameter... paras);

    /**
     * Sets the element identifier string that is used by this query builder as the token to identifier key references
     * in the query string.
     * <p/>
     * For example, in the query 'v.name: Tom' the element identifier is 'v.'
     *
     *
     * @param identifier The element identifier which must not be blank
     * @return This query builder
     */
    public HugeGraphIndexQuery setElementIdentifier(String identifier);

    /**
     * Returns all vertices that match the query in the indexing backend.
     *
     * @return
     */
    public Iterable<Result<HugeGraphVertex>> vertices();

    /**
     * Returns all edges that match the query in the indexing backend.
     *
     * @return
     */
    public Iterable<Result<HugeGraphEdge>> edges();

    /**
     * Returns all properties that match the query in the indexing backend.
     *
     * @return
     */
    public Iterable<Result<HugeGraphVertexProperty>> properties();

    /**
     * Container of a query result with its score.
     * @param <V>
     */
    public interface Result<V extends Element> {

        /**
         * Returns the element that matches the query
         *
         * @return
         */
        public V getElement();

        /**
         * Returns the score of the result with respect to the query (if available)
         * @return
         */
        public double getScore();

    }


}
