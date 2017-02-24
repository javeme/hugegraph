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
import com.baidu.hugegraph.core.HugeGraphEdge;
import com.baidu.hugegraph.core.HugeGraphRelation;
import com.baidu.hugegraph.core.HugeGraphVertex;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IncidenceCondition<E extends HugeGraphRelation> extends Literal<E> {

    private final HugeGraphVertex baseVertex;
    private final HugeGraphVertex otherVertex;

    public IncidenceCondition(HugeGraphVertex baseVertex, HugeGraphVertex otherVertex) {
        Preconditions.checkNotNull(baseVertex);
        Preconditions.checkNotNull(otherVertex);
        this.baseVertex = baseVertex;
        this.otherVertex = otherVertex;
    }

    @Override
    public boolean evaluate(E relation) {
        return relation.isEdge() && ((HugeGraphEdge) relation).otherVertex(baseVertex).equals(otherVertex);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(baseVertex).append(otherVertex).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || !getClass().isInstance(other))
            return false;

        IncidenceCondition oth = (IncidenceCondition) other;
        return baseVertex.equals(oth.baseVertex) && otherVertex.equals(oth.otherVertex);
    }

    @Override
    public String toString() {
        return "incidence[" + baseVertex + "-" + otherVertex + "]";
    }
}
