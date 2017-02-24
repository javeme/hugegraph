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

package com.baidu.hugegraph.graphdb.transaction.indexcache;

import com.baidu.hugegraph.core.PropertyKey;
import com.baidu.hugegraph.core.HugeGraphVertexProperty;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexCache {

    public void add(HugeGraphVertexProperty property);

    public void remove(HugeGraphVertexProperty property);

    public Iterable<HugeGraphVertexProperty> get(Object value, PropertyKey key);

}
