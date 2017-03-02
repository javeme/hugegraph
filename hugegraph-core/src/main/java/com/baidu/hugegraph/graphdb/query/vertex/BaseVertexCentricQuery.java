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

package com.baidu.hugegraph.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.core.HugeGraphRelation;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.SliceQuery;
import com.baidu.hugegraph.graphdb.internal.OrderList;
import com.baidu.hugegraph.graphdb.query.BackendQueryHolder;
import com.baidu.hugegraph.graphdb.query.BaseQuery;
import com.baidu.hugegraph.graphdb.query.QueryUtil;
import com.baidu.hugegraph.graphdb.query.condition.Condition;
import com.baidu.hugegraph.graphdb.query.condition.FixedCondition;
import com.baidu.hugegraph.graphdb.query.profile.ProfileObservable;
import com.baidu.hugegraph.graphdb.query.profile.QueryProfiler;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.List;

/**
 * The base implementation for {@link VertexCentricQuery} which does not yet contain a reference to the
 * base vertex of the query. This query is constructed by {@link BasicVertexCentricQueryBuilder#constructQuery(com.baidu.hugegraph.graphdb.internal.RelationCategory)}
 * and then later extended by single or multi-vertex query which add the vertex to the query.
 * </p>
 * This class override many methods in {@link com.baidu.hugegraph.graphdb.query.ElementQuery} - check there
 * for a description.
 *
 */
public class BaseVertexCentricQuery extends BaseQuery implements ProfileObservable {

    /**
     * The condition of this query in QNF
     */
    protected final Condition<HugeGraphRelation> condition;
    /**
     * The individual component {@link SliceQuery} of this query. This query is considered an OR
     * of the individual components (possibly filtered by the condition if not fitted).
     */
    protected final List<BackendQueryHolder<SliceQuery>> queries;
    /**
     * The result order of this query (if any)
     */
    private final OrderList orders;
    /**
     * The direction condition of this query. This is duplicated from the condition for efficiency reasons.
     */
    protected final Direction direction;

    public BaseVertexCentricQuery(Condition<HugeGraphRelation> condition, Direction direction,
                                  List<BackendQueryHolder<SliceQuery>> queries, OrderList orders,
                                  int limit) {
        super(limit);
        Preconditions.checkArgument(condition != null && queries != null && direction != null);
        Preconditions.checkArgument(QueryUtil.isQueryNormalForm(condition) && limit>=0);
        this.condition = condition;
        this.queries = queries;
        this.orders = orders;
        this.direction=direction;
    }

    protected BaseVertexCentricQuery(BaseVertexCentricQuery query) {
        this(query.getCondition(), query.getDirection(), query.getQueries(), query.getOrders(), query.getLimit());
    }

    /**
     * Construct an empty query
     */
    protected BaseVertexCentricQuery() {
        this(new FixedCondition<HugeGraphRelation>(false), Direction.BOTH, new ArrayList<BackendQueryHolder<SliceQuery>>(0),OrderList.NO_ORDER,0);
    }

    public static BaseVertexCentricQuery emptyQuery() {
        return new BaseVertexCentricQuery();
    }

    public Condition<HugeGraphRelation> getCondition() {
        return condition;
    }

    public OrderList getOrders() {
        return orders;
    }

    public Direction getDirection() {
        return direction;
    }

    protected List<BackendQueryHolder<SliceQuery>> getQueries() {
        return queries;
    }

    public boolean isEmpty() {
        return getLimit()<=0;
    }

    public int numSubQueries() {
        return queries.size();
    }

    /**
     * A query is considered 'simple' if it is comprised of just one sub-query and that query
     * is fitted (i.e. does not require an in-memory filtering).
     * @return
     */
    public boolean isSimple() {
        return queries.size()==1 && queries.get(0).isFitted() && queries.get(0).isSorted();
    }

    public BackendQueryHolder<SliceQuery> getSubQuery(int position) {
        return queries.get(position);
    }

    public boolean matches(HugeGraphRelation relation) {
        return condition.evaluate(relation);
    }

    @Override
    public String toString() {
        String s = "["+condition.toString()+"]";
        if (hasLimit()) s+=":"+getLimit();
        return s;
    }

    @Override
    public void observeWith(QueryProfiler profiler) {
        profiler.setAnnotation(QueryProfiler.CONDITION_ANNOTATION,condition);
        profiler.setAnnotation(QueryProfiler.ORDERS_ANNOTATION,orders);
        if (hasLimit()) profiler.setAnnotation(QueryProfiler.LIMIT_ANNOTATION,getLimit());
        queries.forEach(bqh -> bqh.observeWith(profiler));
    }
}
