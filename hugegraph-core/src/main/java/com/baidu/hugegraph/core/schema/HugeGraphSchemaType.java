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

package com.baidu.hugegraph.core.schema;

/**
 * A HugeGraphSchemaType is a {@link HugeGraphSchemaElement} that represents a label or key used in the graph. As such,
 * a schema type is either a {@link com.baidu.hugegraph.core.RelationType} or a
 * {@link com.baidu.hugegraph.core.VertexLabel}.
 * <p/>
 * HugeGraphSchemaTypes are a special {@link HugeGraphSchemaElement} in that they are referenced from the main graph
 * when creating vertices, edges, and properties.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface HugeGraphSchemaType extends HugeGraphSchemaElement {
}
