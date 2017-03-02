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

import com.baidu.hugegraph.graphdb.tinkerpop.ElementUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Iterator;

/**
 */
public class HugeGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final HugeGraphStepStrategy INSTANCE = new HugeGraphStepStrategy();

    private HugeGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            if (originalGraphStep.getIds() == null || originalGraphStep.getIds().length == 0) {
                //Try to optimize for index calls
                final HugeGraphStep<?, ?> hugeGraphStep = new HugeGraphStep<>(originalGraphStep);
                TraversalHelper.replaceStep(originalGraphStep, (Step) hugeGraphStep, traversal);
                HasStepFolder.foldInHasContainer(hugeGraphStep, traversal);
                HasStepFolder.foldInOrder(hugeGraphStep, traversal, traversal, hugeGraphStep.returnsVertex());
                HasStepFolder.foldInRange(hugeGraphStep, traversal);
            } else {
                //Make sure that any provided "start" elements are instantiated in the current transaction
                Object[] ids = originalGraphStep.getIds();
                ElementUtils.verifyArgsMustBeEitherIdorElement(ids);
                if (ids[0] instanceof Element) {
                    //GraphStep constructor ensures that the entire array is elements
                    final Object[] elementIds = new Object[ids.length];
                    for (int i = 0; i < ids.length; i++) {
                        elementIds[i] = ((Element) ids[i]).id();
                    }
                    originalGraphStep.setIteratorSupplier(() -> (Iterator) (originalGraphStep.returnsVertex() ?
                            ((Graph) originalGraphStep.getTraversal().getGraph().get()).vertices(elementIds) :
                            ((Graph) originalGraphStep.getTraversal().getGraph().get()).edges(elementIds)));
                }
            }
        });
    }

    public static HugeGraphStepStrategy instance() {
        return INSTANCE;
    }
}
