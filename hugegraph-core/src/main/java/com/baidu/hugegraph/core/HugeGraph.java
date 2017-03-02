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

import com.baidu.hugegraph.core.schema.HugeGraphManagement;
import com.baidu.hugegraph.graphdb.configuration.HugeGraphConstants;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.Gremlin;

/**
 * HugeGraph graph database implementation of the Blueprint's interface.
 * Use {@link HugeGraphFactory} to open and configure HugeGraph instances.
 *
 * @see HugeGraphFactory
 * @see HugeGraphTransaction
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_PERFORMANCE)
@Graph.OptIn("com.baidu.hugegraph.blueprints.process.traversal.strategy.HugeGraphStrategySuite")
//------------------------
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.VertexPropertyTest$VertexPropertyAddition",
        method = "shouldHandleSetVertexProperties",
        reason = "HugeGraph can only handle SET cardinality for properties when defined in the schema.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldOnlyAllowReadingVertexPropertiesInMapReduce",
        reason = "HugeGraph simply throws the wrong exception -- should not be a ReadOnly transaction exception but a specific one for MapReduce. This is too cumbersome to refactor in HugeGraph.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldProcessResultGraphNewWithPersistVertexProperties",
        reason = "The result graph should return an empty iterator when vertex.edges() or vertex.vertices() is called.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest",
        method = "shouldReadGraphMLWithNoEdgeLabels",
        reason = "HugeGraph does not support default edge label (edge) used when GraphML is missing edge labels.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldSupportGraphFilter",
        reason = "HugeGraph test graph computer (FulgoraGraphComputer) " +
            "currently does not support graph filters but does not throw proper exception because doing so breaks numerous " +
            "tests in gremlin-test ProcessComputerSuite.")
public interface HugeGraph extends Transaction {

   /* ---------------------------------------------------------------
    * Transactions and general admin
    * ---------------------------------------------------------------
    */

    /**
     * Opens a new thread-independent {@link HugeGraphTransaction}.
     * <p/>
     * The transaction is open when it is returned but MUST be explicitly closed by calling {@link com.baidu.hugegraph.core.HugeGraphTransaction#commit()}
     * or {@link com.baidu.hugegraph.core.HugeGraphTransaction#rollback()} when it is no longer needed.
     * <p/>
     * Note, that this returns a thread independent transaction object. It is not necessary to call this method
     * to use Blueprint's standard transaction framework which will automatically start a transaction with the first
     * operation on the graph.
     *
     * @return Transaction object representing a transactional context.
     */
    public HugeGraphTransaction newTransaction();

    /**
     * Returns a {@link TransactionBuilder} to construct a new thread-independent {@link HugeGraphTransaction}.
     *
     * @return a new TransactionBuilder
     * @see TransactionBuilder
     * @see #newTransaction()
     */
    public TransactionBuilder buildTransaction();

    /**
     * Returns the management system for this graph instance. The management system provides functionality
     * to change global configuration options, install indexes and inspect the graph schema.
     * <p />
     * The management system operates in its own transactional context which must be explicitly closed.
     *
     * @return
     */
    public HugeGraphManagement openManagement();

    /**
     * Checks whether the graph is open.
     *
     * @return true, if the graph is open, else false.
     * @see #close()
     */
    public boolean isOpen();

    /**
     * Checks whether the graph is closed.
     *
     * @return true, if the graph has been closed, else false
     */
    public boolean isClosed();

    /**
     * Closes the graph database.
     * <p/>
     * Closing the graph database causes a disconnect and possible closing of the underlying storage backend
     * and a release of all occupied resources by this graph database.
     * Closing a graph database requires that all open thread-independent transactions have been closed -
     * otherwise they will be left abandoned.
     *
     * @throws HugeGraphException if closing the graph database caused errors in the storage backend
     */
    @Override
    public void close() throws HugeGraphException;

    /**
     * The version of this HugeGraph graph database
     *
     * @return
     */
    public static String version() {
        return HugeGraphConstants.VERSION;
    }

    public static void main(String[] args) {
        System.out.println("HugeGraph " + HugeGraph.version() + ", Apache TinkerPop " + Gremlin.version());
    }
}
