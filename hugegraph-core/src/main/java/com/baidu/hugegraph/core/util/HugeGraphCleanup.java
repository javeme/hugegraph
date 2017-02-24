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

package com.baidu.hugegraph.core.util;

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.core.HugeGraph;

import com.baidu.hugegraph.diskstorage.util.BackendOperation;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;

import java.time.Duration;
import java.util.concurrent.Callable;

/**
 * Utility class containing methods that simplify HugeGraph clean-up processes.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HugeGraphCleanup {

    /**
     * Clears out the entire graph. This will delete ALL of the data stored in this graph and the data will NOT be
     * recoverable. This method is intended only for development and testing use.
     *
     * @param graph
     * @throws IllegalArgumentException if the graph has not been shut down
     * @throws com.baidu.hugegraph.core.HugeGraphException if clearing the storage is unsuccessful
     */
    public static final void clear(HugeGraph graph) {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(graph instanceof StandardHugeGraph, "Invalid graph instance detected: %s",
                graph.getClass());
        StandardHugeGraph g = (StandardHugeGraph) graph;
        Preconditions.checkArgument(!g.isOpen(), "Graph needs to be shut down before it can be cleared.");
        final GraphDatabaseConfiguration config = g.getConfiguration();
        BackendOperation.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                config.getBackend().clearStorage();
                return true;
            }

            @Override
            public String toString() {
                return "ClearBackend";
            }
        }, Duration.ofSeconds(20));
    }

}
