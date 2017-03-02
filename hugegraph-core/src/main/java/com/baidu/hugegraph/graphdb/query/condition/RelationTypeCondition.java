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

package com.baidu.hugegraph.graphdb.query.condition;

import com.google.common.base.Preconditions;
import com.baidu.hugegraph.core.RelationType;
import com.baidu.hugegraph.core.HugeGraphElement;
import com.baidu.hugegraph.core.HugeGraphRelation;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 *
 *
 */
public class RelationTypeCondition<E extends HugeGraphElement> extends Literal<E> {

    private final RelationType relationType;

    public RelationTypeCondition(RelationType relationType) {
        Preconditions.checkNotNull(relationType);
        this.relationType = relationType;
    }

    @Override
    public boolean evaluate(E element) {
        Preconditions.checkArgument(element instanceof HugeGraphRelation);
        return relationType.equals(((HugeGraphRelation)element).getType());
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(relationType).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other)) && relationType.equals(((RelationTypeCondition) other).relationType);
    }

    @Override
    public String toString() {
        return "type["+ relationType.toString()+"]";
    }
}
