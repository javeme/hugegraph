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

import com.baidu.hugegraph.core.PropertyKey;
import com.baidu.hugegraph.core.schema.HugeGraphSchemaType;
import com.baidu.hugegraph.graphdb.internal.ElementCategory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IndexType {

    public ElementCategory getElement();

    public IndexField[] getFieldKeys();

    public IndexField getField(PropertyKey key);

    public boolean indexesKey(PropertyKey key);

    public boolean isCompositeIndex();

    public boolean isMixedIndex();

    public boolean hasSchemaTypeConstraint();

    public HugeGraphSchemaType getSchemaTypeConstraint();

    public String getBackingIndexName();

    public String getName();

    /**
     * Resets the internal caches used to speed up lookups on this index. This is needed when the index gets modified in
     * {@link com.baidu.hugegraph.graphdb.database.management.ManagementSystem}.
     */
    public void resetCache();

    // TODO: Add in the future
    // public And getCondition();

}
