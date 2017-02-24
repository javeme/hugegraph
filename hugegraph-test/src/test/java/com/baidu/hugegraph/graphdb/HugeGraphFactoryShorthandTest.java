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

package com.baidu.hugegraph.graphdb;

import com.baidu.hugegraph.core.HugeGraphFactory;
import com.baidu.hugegraph.core.HugeGraph;
import org.junit.Test;

/**
 * Tests HugeGraphFactory.open's colon-delimited shorthand parameter syntax.
 *
 * This class contains only one method so that it will run in a separate surefire fork. This is useful for checking
 * acyclic static initializer invocation on the shorthand path (#831).
 */
public class HugeGraphFactoryShorthandTest {

    @Test
    public void testHugeGraphFactoryShorthand() {
        HugeGraph g = HugeGraphFactory.open("inmemory");
        g.close();
    }
}
