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

package com.baidu.hugegraph.graphdb.olap.computer;

import com.baidu.hugegraph.core.HugeGraphTransaction;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraElementTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    private final HugeGraphTransaction graph;

    private FulgoraElementTraversal(final HugeGraphTransaction graph) {
        super(graph);
        this.graph = graph;
    }

    public static <S, E> FulgoraElementTraversal<S, E> of(final HugeGraphTransaction graph) {
        return new FulgoraElementTraversal<>(graph);
    }

    @Override
    public Optional<Graph> getGraph() {
        return Optional.of(graph);
    }

}
