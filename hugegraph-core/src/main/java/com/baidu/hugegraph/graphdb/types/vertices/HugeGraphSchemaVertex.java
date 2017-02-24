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

package com.baidu.hugegraph.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.baidu.hugegraph.core.HugeGraphEdge;
import com.baidu.hugegraph.core.HugeGraphVertexProperty;
import com.baidu.hugegraph.core.HugeGraphVertex;
import com.baidu.hugegraph.core.HugeGraphVertexQuery;
import com.baidu.hugegraph.core.schema.SchemaStatus;
import com.baidu.hugegraph.graphdb.internal.HugeGraphSchemaCategory;
import com.baidu.hugegraph.graphdb.transaction.RelationConstructor;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import com.baidu.hugegraph.graphdb.types.*;
import com.baidu.hugegraph.graphdb.types.indextype.CompositeIndexTypeWrapper;
import com.baidu.hugegraph.graphdb.types.indextype.MixedIndexTypeWrapper;
import com.baidu.hugegraph.graphdb.types.system.BaseKey;
import com.baidu.hugegraph.graphdb.types.system.BaseLabel;
import com.baidu.hugegraph.graphdb.vertices.CacheVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;

public class HugeGraphSchemaVertex extends CacheVertex implements SchemaSource {

    public HugeGraphSchemaVertex(StandardHugeGraphTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    private String name = null;

    @Override
    public String name() {
        if (name == null) {
            HugeGraphVertexProperty<String> p;
            if (isLoaded()) {
                StandardHugeGraphTx tx = tx();
                p = (HugeGraphVertexProperty) Iterables.getOnlyElement(RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseKey.SchemaName, Direction.OUT),
                        tx), null);
            } else {
                p = Iterables.getOnlyElement(query().type(BaseKey.SchemaName).properties(), null);
            }
            Preconditions.checkState(p != null, "Could not find type for id: %s", longId());
            name = p.value();
        }
        assert name != null;
        return HugeGraphSchemaCategory.getName(name);
    }

    @Override
    protected Vertex getVertexLabelInternal() {
        return null;
    }

    private TypeDefinitionMap definition = null;

    @Override
    public TypeDefinitionMap getDefinition() {
        TypeDefinitionMap def = definition;
        if (def == null) {
            def = new TypeDefinitionMap();
            Iterable<HugeGraphVertexProperty> ps;
            if (isLoaded()) {
                StandardHugeGraphTx tx = tx();
                ps = (Iterable) RelationConstructor.readRelation(this, tx.getGraph().getSchemaCache()
                        .getSchemaRelations(longId(), BaseKey.SchemaDefinitionProperty, Direction.OUT), tx);
            } else {
                ps = query().type(BaseKey.SchemaDefinitionProperty).properties();
            }
            for (HugeGraphVertexProperty property : ps) {
                TypeDefinitionDescription desc = property.valueOrNull(BaseKey.SchemaDefinitionDesc);
                Preconditions.checkArgument(desc != null && desc.getCategory().isProperty());
                def.setValue(desc.getCategory(), property.value());
            }
            assert def.size() > 0;
            definition = def;
        }
        assert def != null;
        return def;
    }

    private ListMultimap<TypeDefinitionCategory, Entry> outRelations = null;
    private ListMultimap<TypeDefinitionCategory, Entry> inRelations = null;

    @Override
    public Iterable<Entry> getRelated(TypeDefinitionCategory def, Direction dir) {
        assert dir == Direction.OUT || dir == Direction.IN;
        ListMultimap<TypeDefinitionCategory, Entry> rels = dir == Direction.OUT ? outRelations : inRelations;
        if (rels == null) {
            ImmutableListMultimap.Builder<TypeDefinitionCategory, Entry> b = ImmutableListMultimap.builder();
            Iterable<HugeGraphEdge> edges;
            if (isLoaded()) {
                StandardHugeGraphTx tx = tx();
                edges = (Iterable) RelationConstructor.readRelation(this, tx.getGraph().getSchemaCache()
                        .getSchemaRelations(longId(), BaseLabel.SchemaDefinitionEdge, dir), tx);
            } else {
                edges = query().type(BaseLabel.SchemaDefinitionEdge).direction(dir).edges();
            }
            for (HugeGraphEdge edge : edges) {
                HugeGraphVertex oth = edge.vertex(dir.opposite());
                assert oth instanceof HugeGraphSchemaVertex;
                TypeDefinitionDescription desc = edge.valueOrNull(BaseKey.SchemaDefinitionDesc);
                Object modifier = null;
                if (desc.getCategory().hasDataType()) {
                    assert desc.getModifier() != null
                            && desc.getModifier().getClass().equals(desc.getCategory().getDataType());
                    modifier = desc.getModifier();
                }
                b.put(desc.getCategory(), new Entry((HugeGraphSchemaVertex) oth, modifier));
            }
            rels = b.build();
            if (dir == Direction.OUT)
                outRelations = rels;
            else
                inRelations = rels;
        }
        assert rels != null;
        return rels.get(def);
    }

    /**
     * Resets the internal caches used to speed up lookups on this index type. This is needed when the type gets
     * modified in the {@link com.baidu.hugegraph.graphdb.database.management.ManagementSystem}.
     */
    @Override
    public void resetCache() {
        name = null;
        definition = null;
        outRelations = null;
        inRelations = null;
    }

    public Iterable<HugeGraphEdge> getEdges(final TypeDefinitionCategory def, final Direction dir) {
        return getEdges(def, dir, null);
    }

    public Iterable<HugeGraphEdge> getEdges(final TypeDefinitionCategory def, final Direction dir,
            HugeGraphSchemaVertex other) {
        HugeGraphVertexQuery query = query().type(BaseLabel.SchemaDefinitionEdge).direction(dir);
        if (other != null)
            query.adjacent(other);
        return Iterables.filter(query.edges(), new Predicate<HugeGraphEdge>() {
            @Override
            public boolean apply(@Nullable HugeGraphEdge edge) {
                TypeDefinitionDescription desc = edge.valueOrNull(BaseKey.SchemaDefinitionDesc);
                return desc.getCategory() == def;
            }
        });
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public SchemaStatus getStatus() {
        return getDefinition().getValue(TypeDefinitionCategory.STATUS, SchemaStatus.class);
    }

    @Override
    public IndexType asIndexType() {
        Preconditions.checkArgument(getDefinition().containsKey(TypeDefinitionCategory.INTERNAL_INDEX),
                "Schema vertex is not a type vertex: [%s,%s]", longId(), name());
        if (getDefinition().<Boolean> getValue(TypeDefinitionCategory.INTERNAL_INDEX)) {
            return new CompositeIndexTypeWrapper(this);
        } else {
            return new MixedIndexTypeWrapper(this);
        }
    }

}
