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

package com.baidu.hugegraph.diskstorage.log.kcvs;

import com.google.common.collect.Lists;
import com.baidu.hugegraph.core.HugeGraphException;
import com.baidu.hugegraph.diskstorage.BackendException;
import com.baidu.hugegraph.diskstorage.Entry;
import com.baidu.hugegraph.diskstorage.StaticBuffer;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import com.baidu.hugegraph.diskstorage.keycolumnvalue.cache.KCVSCache;

/**
 */
public class ExternalCachePersistor implements ExternalPersistor {

    private final KCVSCache kcvs;
    private final CacheTransaction tx;

    public ExternalCachePersistor(KCVSCache kcvs, CacheTransaction tx) {
        this.kcvs = kcvs;
        this.tx = tx;
    }

    @Override
    public void add(StaticBuffer key, Entry cell) {
        try {
            kcvs.mutateEntries(key, Lists.newArrayList(cell), KCVSCache.NO_DELETIONS,tx);
        } catch (BackendException e) {
            throw new HugeGraphException("Unexpected storage exception in log persistence against cache",e);
        }
    }
}
