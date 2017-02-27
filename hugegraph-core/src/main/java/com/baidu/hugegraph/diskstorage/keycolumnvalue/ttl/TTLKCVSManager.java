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

package com.baidu.hugegraph.diskstorage.keycolumnvalue.ttl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.baidu.hugegraph.diskstorage.BackendException;
import com.baidu.hugegraph.diskstorage.Entry;
import com.baidu.hugegraph.diskstorage.EntryMetaData;
import com.baidu.hugegraph.diskstorage.MetaAnnotatable;
import com.baidu.hugegraph.diskstorage.StaticBuffer;
import com.baidu.hugegraph.diskstorage.StoreMetaData;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.KCVMutation;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.KCVSManagerProxy;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.StoreFeatures;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.Collection;
import java.util.Map;

/**
 * Turns a store with fine-grained cell-level TTL support into a store
 * with coarse-grained store-level (columnfamily-level) TTL support.
 * Useful when running a KCVSLog atop Cassandra.  Cassandra has
 * cell-level TTL support, but KCVSLog just wants to write all of its
 * data with a fixed, CF-wide TTL.  This class stores a fixed TTL set
 * during construction and applies it to every entry written through
 * subsequent mutate/mutateMany calls.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TTLKCVSManager extends KCVSManagerProxy {

    private final StoreFeatures features;
    private final Map<String, Integer> ttlEnabledStores = Maps.newConcurrentMap();

    public TTLKCVSManager(KeyColumnValueStoreManager manager) {
        super(manager);
        Preconditions.checkArgument(manager.getFeatures().hasCellTTL());
        Preconditions.checkArgument(!manager.getFeatures().hasStoreTTL(),
                "Using TTLKCVSManager with %s is redundant: underlying implementation already supports store-level ttl",
                manager);
        this.features = new StandardStoreFeatures.Builder(manager.getFeatures()).storeTTL(true).build();
    }

    /**
     * Returns true if the parameter supports at least one of the following:
     *
     * <ul>
     * <li>cell-level TTL {@link StoreFeatures#hasCellTTL()}</li>
     * <li>store-level TTL {@link StoreFeatures#hasStoreTTL()}</li>
     * </ul>
     *
     * @param features an arbitrary {@code StoreFeatures} instance
     * @return true if and only if at least one TTL mode is supported
     */
    public static boolean supportsAnyTTL(StoreFeatures features) {
        return features.hasCellTTL() || features.hasStoreTTL();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name) throws BackendException {
        return openDatabase(name, StoreMetaData.EMPTY);
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        KeyColumnValueStore store = manager.openDatabase(name);
        int storeTTL = -1;
        if (metaData.contains(StoreMetaData.TTL)) {
            storeTTL = (Integer) metaData.get(StoreMetaData.TTL);
        }
        Preconditions.checkArgument(storeTTL>0,"TTL must be positive: %s", storeTTL);
        ttlEnabledStores.put(name, storeTTL);
        return new TTLKCVS(store, storeTTL);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        if (!manager.getFeatures().hasStoreTTL()) {
            assert manager.getFeatures().hasCellTTL();
            for (Map.Entry<String,Map<StaticBuffer, KCVMutation>> sentry : mutations.entrySet()) {
                Integer ttl = ttlEnabledStores.get(sentry.getKey());
                if (null != ttl && 0 < ttl) {
                    for (KCVMutation mut : sentry.getValue().values()) {
                        if (mut.hasAdditions()) applyTTL(mut.getAdditions(), ttl);
                    }
                }
            }
        }
        manager.mutateMany(mutations,txh);
    }

    public static void applyTTL(Collection<Entry> additions, int ttl) {
        for (Entry entry : additions) {
            assert entry instanceof MetaAnnotatable;
            ((MetaAnnotatable)entry).setMetaData(EntryMetaData.TTL, ttl);
        }
    }


}
