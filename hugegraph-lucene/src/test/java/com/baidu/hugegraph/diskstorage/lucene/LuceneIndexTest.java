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

package com.baidu.hugegraph.diskstorage.lucene;

import com.baidu.hugegraph.StorageSetup;
import com.baidu.hugegraph.core.Cardinality;
import com.baidu.hugegraph.core.schema.Parameter;
import com.baidu.hugegraph.core.attribute.*;
import com.baidu.hugegraph.diskstorage.BackendException;
import com.baidu.hugegraph.diskstorage.configuration.Configuration;
import com.baidu.hugegraph.diskstorage.configuration.ModifiableConfiguration;
import com.baidu.hugegraph.diskstorage.indexing.IndexProvider;
import com.baidu.hugegraph.diskstorage.indexing.IndexProviderTest;
import com.baidu.hugegraph.core.schema.Mapping;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 */

public class LuceneIndexTest extends IndexProviderTest {

    private static final Logger log =
            LoggerFactory.getLogger(LuceneIndexTest.class);

    @Rule
    public TestName methodName = new TestName();

    @Override
    public IndexProvider openIndex() throws BackendException {
        return new LuceneIndex(getLocalLuceneTestConfig());
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return false;
    }

    public static final Configuration getLocalLuceneTestConfig() {
        final String index = "lucene";
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, StorageSetup.getHomeDir("lucene"),index);
        return config.restrictTo(index);
    }

    @Test
    public void testSupport() {
        // DEFAULT(=TEXT) support
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS_PREFIX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Text.CONTAINS_REGEX)); // TODO Not supported yet
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Text.REGEX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Text.PREFIX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE), Cmp.NOT_EQUAL));

        // Same tests as above, except explicitly specifying TEXT instead of relying on DEFAULT
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_PREFIX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.CONTAINS_REGEX)); // TODO Not supported yet
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.REGEX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Text.PREFIX));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Cmp.EQUAL));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.TEXT)), Cmp.NOT_EQUAL));

        // STRING support
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.CONTAINS));
        assertFalse(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.CONTAINS_PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.REGEX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Text.PREFIX));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Cmp.EQUAL));
        assertTrue(index.supports(of(String.class, Cardinality.SINGLE, new Parameter("mapping", Mapping.STRING)), Cmp.NOT_EQUAL));

        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.LESS_THAN_EQUAL));
        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.LESS_THAN));
        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.GREATER_THAN));
        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.GREATER_THAN_EQUAL));
        assertTrue(index.supports(of(Date.class, Cardinality.SINGLE), Cmp.NOT_EQUAL));

        assertTrue(index.supports(of(Boolean.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertTrue(index.supports(of(Boolean.class, Cardinality.SINGLE), Cmp.NOT_EQUAL));

        assertTrue(index.supports(of(UUID.class, Cardinality.SINGLE), Cmp.EQUAL));
        assertTrue(index.supports(of(UUID.class, Cardinality.SINGLE), Cmp.NOT_EQUAL));
    }

//    @Override
//    public void testDeleteDocumentThenModifyField() {
//        // This fails under Lucene but works in ES
//        log.info("Skipping " + getClass().getSimpleName() + "." + methodName.getMethodName());
//    }
}
