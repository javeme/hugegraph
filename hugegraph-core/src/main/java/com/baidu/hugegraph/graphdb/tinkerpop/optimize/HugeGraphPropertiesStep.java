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

package com.baidu.hugegraph.graphdb.tinkerpop.optimize;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.baidu.hugegraph.core.*;
import com.baidu.hugegraph.graphdb.query.BaseQuery;
import com.baidu.hugegraph.graphdb.query.Query;
import com.baidu.hugegraph.graphdb.query.HugeGraphPredicate;
import com.baidu.hugegraph.graphdb.query.profile.QueryProfiler;
import com.baidu.hugegraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import com.baidu.hugegraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HugeGraphPropertiesStep<E> extends PropertiesStep<E> implements HasStepFolder<Element, E>, Profiling, MultiQueriable<Element,E> {

    public HugeGraphPropertiesStep(PropertiesStep<E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnType(), originalStep.getPropertyKeys());
        originalStep.getLabels().forEach(this::addLabel);
        this.hasContainers = new ArrayList<>();
        this.limit = Query.NO_LIMIT;
    }

    private boolean initialized = false;
    private boolean useMultiQuery = false;
    private Map<HugeGraphVertex, Iterable<? extends HugeGraphProperty>> multiQueryResults = null;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;


    @Override
    public void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = useMultiQuery;
    }

    private <Q extends BaseVertexQuery> Q makeQuery(Q query) {
        String[] keys = getPropertyKeys();
        query.keys(keys);
        for (HasContainer condition : hasContainers) {
            query.has(condition.getKey(), HugeGraphPredicate.Converter.convert(condition.getBiPredicate()), condition.getValue());
        }
        for (OrderEntry order : orders) query.orderBy(order.key, order.order);
        if (limit != BaseQuery.NO_LIMIT) query.limit(limit);
        ((BasicVertexCentricQueryBuilder) query).profiler(queryProfiler);
        return query;
    }

    private Iterator<E> convertIterator(Iterable<? extends HugeGraphProperty> iterable) {
        if (getReturnType().forProperties()) return (Iterator<E>) iterable.iterator();
        assert getReturnType().forValues();
        return (Iterator<E>) Iterators.transform(iterable.iterator(), p -> ((HugeGraphProperty) p).value());
    }

    @SuppressWarnings("deprecation")
    private void initialize() {
        assert !initialized;
        initialized = true;
        assert getReturnType().forProperties() || (orders.isEmpty() && hasContainers.isEmpty());

        if (!starts.hasNext()) throw FastNoSuchElementException.instance();
        List<Traverser.Admin<Element>> elements = new ArrayList<>();
        starts.forEachRemaining(v -> elements.add(v));
        starts.add(elements.iterator());
        assert elements.size() > 0;

        useMultiQuery = useMultiQuery && elements.stream().noneMatch(e -> !(e.get() instanceof Vertex));

        if (useMultiQuery) {
            HugeGraphMultiVertexQuery mquery = HugeGraphTraversalUtil.getTx(traversal).multiQuery();
            elements.forEach(e -> mquery.addVertex((Vertex) e.get()));
            makeQuery(mquery);

            multiQueryResults = mquery.properties();
        }
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        if (!initialized) initialize();
        return super.processNextStart();
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Element> traverser) {
        if (useMultiQuery) { //it is guaranteed that all elements are vertices
            assert multiQueryResults != null;
            return convertIterator(multiQueryResults.get(traverser.get()));
        } else if (traverser.get() instanceof HugeGraphVertex || traverser.get() instanceof WrappedVertex) {
            HugeGraphVertexQuery query = makeQuery((HugeGraphTraversalUtil.getHugeGraphVertex(traverser)).query());
            return convertIterator(query.properties());
        } else {
            //It is some other element (edge or vertex property)
            Iterator<E> iter;
            if (getReturnType().forValues()) {
                assert orders.isEmpty() && hasContainers.isEmpty();
                iter = traverser.get().values(getPropertyKeys());
            } else {
                //this asks for properties
                assert orders.isEmpty();
                //HasContainers don't apply => empty result set
                if (!hasContainers.isEmpty()) return Collections.emptyIterator();
                iter = (Iterator<E>) traverser.get().properties(getPropertyKeys());
            }
            if (limit!=Query.NO_LIMIT) iter = Iterators.limit(iter,limit);
            return iter;
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
    }

    @Override
    public HugeGraphPropertiesStep<E> clone() {
        final HugeGraphPropertiesStep<E> clone = (HugeGraphPropertiesStep<E>) super.clone();
        clone.initialized = false;
        return clone;
    }

    /*
    ===== HOLDER =====
     */

    private final List<HasContainer> hasContainers;
    private int limit = BaseQuery.NO_LIMIT;
    private List<HasStepFolder.OrderEntry> orders = new ArrayList<>();


    @Override
    public void addAll(Iterable<HasContainer> has) {
        Iterables.addAll(hasContainers, has);
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new HasStepFolder.OrderEntry(key, order));
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public int getLimit() {
        return this.limit;
    }

    @Override
    public String toString() {
        return this.hasContainers.isEmpty() ? super.toString() : StringFactory.stepString(this, this.hasContainers);
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }
}
