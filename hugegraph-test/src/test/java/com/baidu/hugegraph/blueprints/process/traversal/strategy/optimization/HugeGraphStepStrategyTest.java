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

package com.baidu.hugegraph.blueprints.process.traversal.strategy.optimization;

import com.baidu.hugegraph.blueprints.InMemoryGraphProvider;
import com.baidu.hugegraph.core.HugeGraph;
import com.baidu.hugegraph.graphdb.tinkerpop.optimize.HugeGraphStep;

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = InMemoryGraphProvider.class, graph = HugeGraph.class)
public class HugeGraphStepStrategyTest extends AbstractGremlinProcessTest {

    @Test
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void shouldFoldInHasContainers() {
        GraphTraversal.Admin traversal = g.V().has("name", "marko").asAdmin();
        assertEquals(2, traversal.getSteps().size());
        assertEquals(HasStep.class, traversal.getEndStep().getClass());
        traversal.applyStrategies();
        assertEquals(1, traversal.getSteps().size());
        assertEquals(HugeGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(HugeGraphStep.class, traversal.getEndStep().getClass());
        assertEquals(1, ((HugeGraphStep) traversal.getStartStep()).getHasContainers().size());
        assertEquals("name", ((HugeGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
        assertEquals("marko", ((HugeGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
        ////
        traversal = g.V().has("name", "marko").has("age", P.gt(20)).asAdmin();
        traversal.applyStrategies();
        assertEquals(1, traversal.getSteps().size());
        assertEquals(HugeGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(2, ((HugeGraphStep) traversal.getStartStep()).getHasContainers().size());
        ////
        traversal = g.V().has("name", "marko").out().has("name", "daniel").asAdmin();
        traversal.applyStrategies();
        assertEquals(3, traversal.getSteps().size());
        assertEquals(HugeGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(1, ((HugeGraphStep) traversal.getStartStep()).getHasContainers().size());
        assertEquals("name", ((HugeGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
        assertEquals("marko", ((HugeGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
        assertEquals(HasStep.class, traversal.getEndStep().getClass());
        ////
        traversal = g.V().has("name", "marko").out().V().has("name", "daniel").asAdmin();
        traversal.applyStrategies();
        assertEquals(3, traversal.getSteps().size());
        assertEquals(HugeGraphStep.class, traversal.getStartStep().getClass());
        assertEquals(1, ((HugeGraphStep) traversal.getStartStep()).getHasContainers().size());
        assertEquals("name", ((HugeGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
        assertEquals("marko", ((HugeGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
        assertEquals(HugeGraphStep.class, traversal.getSteps().get(2).getClass());
        assertEquals(1, ((HugeGraphStep) traversal.getSteps().get(2)).getHasContainers().size());
        assertEquals("name", ((HugeGraphStep<?, ?>) traversal.getSteps().get(2)).getHasContainers().get(0).getKey());
        assertEquals("daniel", ((HugeGraphStep<?,?>) traversal.getSteps().get(2)).getHasContainers().get(0).getValue());
        assertEquals(HugeGraphStep.class, traversal.getEndStep().getClass());
    }

}
