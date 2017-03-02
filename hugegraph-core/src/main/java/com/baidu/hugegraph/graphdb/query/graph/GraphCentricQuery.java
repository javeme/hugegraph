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

package com.baidu.hugegraph.graphdb.query.graph;

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.core.HugeGraphElement;
import com.baidu.hugegraph.graphdb.internal.ElementCategory;
import com.baidu.hugegraph.graphdb.internal.OrderList;
import com.baidu.hugegraph.graphdb.query.BackendQueryHolder;
import com.baidu.hugegraph.graphdb.query.BaseQuery;
import com.baidu.hugegraph.graphdb.query.ElementQuery;
import com.baidu.hugegraph.graphdb.query.QueryUtil;
import com.baidu.hugegraph.graphdb.query.condition.Condition;
import com.baidu.hugegraph.graphdb.query.condition.FixedCondition;
import com.baidu.hugegraph.graphdb.query.profile.ProfileObservable;
import com.baidu.hugegraph.graphdb.query.profile.QueryProfiler;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Comparator;

/**
 * An executable {@link ElementQuery} for {@link com.baidu.hugegraph.core.HugeGraphQuery}. This query contains
 * the condition, and only one sub-query {@link JointIndexQuery}.
 * It also maintains the ordering for the query result which is needed by the {@link com.baidu.hugegraph.graphdb.query.QueryProcessor}
 * to correctly order the result.
 *
 */
public class GraphCentricQuery extends BaseQuery implements ElementQuery<HugeGraphElement, JointIndexQuery>, ProfileObservable {

    /**
     * The condition of this query, the result set is the set of all elements in the graph for which this
     * condition evaluates to true.
     */
    private final Condition<HugeGraphElement> condition;
    /**
     * The {@link JointIndexQuery} to execute against the indexing backends and index store.
     */
    private final BackendQueryHolder<JointIndexQuery> indexQuery;
    /**
     * The result order of this query (if any)
     */
    private final OrderList orders;
    /**
     * The type of element this query is asking for: vertex, edge, or property.
     */
    private final ElementCategory resultType;

    public GraphCentricQuery(ElementCategory resultType, Condition<HugeGraphElement> condition, OrderList orders,
                             BackendQueryHolder<JointIndexQuery> indexQuery, int limit) {
        super(limit);
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(orders != null && orders.isImmutable());
        Preconditions.checkArgument(QueryUtil.isQueryNormalForm(condition));
        Preconditions.checkNotNull(resultType);
        Preconditions.checkNotNull(indexQuery);
        this.condition = condition;
        this.orders = orders;
        this.resultType = resultType;
        this.indexQuery = indexQuery;
    }

    public static final GraphCentricQuery emptyQuery(ElementCategory resultType) {
        Condition<HugeGraphElement> cond = new FixedCondition<HugeGraphElement>(false);
        return new GraphCentricQuery(resultType, cond, OrderList.NO_ORDER,
                new BackendQueryHolder<JointIndexQuery>(new JointIndexQuery(),
                        true, false), 0);
    }

    public Condition<HugeGraphElement> getCondition() {
        return condition;
    }

    public ElementCategory getResultType() {
        return resultType;
    }

    public OrderList getOrder() {
        return orders;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[").append(condition.toString()).append("]");
        if (!orders.isEmpty()) b.append(getLimit());
        if (hasLimit()) b.append("(").append(getLimit()).append(")");
        b.append(":").append(resultType.toString());
        return b.toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(condition).append(resultType).append(orders).append(getLimit()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (other == null) return false;
        else if (!getClass().isInstance(other)) return false;
        GraphCentricQuery oth = (GraphCentricQuery) other;
        return resultType == oth.resultType && condition.equals(oth.condition) &&
                orders.equals(oth.getOrder()) && getLimit() == oth.getLimit();
    }

    @Override
    public boolean isEmpty() {
        return getLimit() <= 0;
    }

    @Override
    public int numSubQueries() {
        return 1;
    }

    @Override
    public BackendQueryHolder<JointIndexQuery> getSubQuery(int position) {
        if (position == 0) return indexQuery;
        else throw new IndexOutOfBoundsException();
    }

    @Override
    public boolean isSorted() {
        return !orders.isEmpty();
    }

    @Override
    public Comparator<HugeGraphElement> getSortOrder() {
        if (orders.isEmpty()) return new ComparableComparator();
        else return orders;
    }

    @Override
    public boolean hasDuplicateResults() {
        return false;
    }

    @Override
    public boolean matches(HugeGraphElement element) {
        return condition.evaluate(element);
    }


    @Override
    public void observeWith(QueryProfiler profiler) {
        profiler.setAnnotation(QueryProfiler.CONDITION_ANNOTATION,condition);
        profiler.setAnnotation(QueryProfiler.ORDERS_ANNOTATION,orders);
        if (hasLimit()) profiler.setAnnotation(QueryProfiler.LIMIT_ANNOTATION,getLimit());
        indexQuery.observeWith(profiler);
    }
}
