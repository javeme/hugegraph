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

package com.baidu.hugegraph.graphdb.types;

import com.baidu.hugegraph.core.EdgeLabel;
import com.baidu.hugegraph.core.PropertyKey;
import com.baidu.hugegraph.core.RelationType;
import com.baidu.hugegraph.core.VertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TypeInspector {

    public default PropertyKey getExistingPropertyKey(long id) {
        return (PropertyKey) getExistingRelationType(id);
    }

    public default EdgeLabel getExistingEdgeLabel(long id) {
        return (EdgeLabel) getExistingRelationType(id);
    }

    public RelationType getExistingRelationType(long id);

    public VertexLabel getExistingVertexLabel(long id);

    public boolean containsRelationType(String name);

    public RelationType getRelationType(String name);

}
