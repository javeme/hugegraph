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

package com.baidu.hugegraph.graphdb.database.indexing;

import com.baidu.hugegraph.core.attribute.Cmp;
import com.baidu.hugegraph.core.attribute.Contain;
import com.baidu.hugegraph.diskstorage.indexing.IndexFeatures;
import com.baidu.hugegraph.diskstorage.indexing.IndexInformation;
import com.baidu.hugegraph.diskstorage.indexing.KeyInformation;
import com.baidu.hugegraph.graphdb.query.HugeGraphPredicate;

/**
 */

public class StandardIndexInformation implements IndexInformation {

    public static final StandardIndexInformation INSTANCE = new StandardIndexInformation();

    private static final IndexFeatures STANDARD_FEATURES = new IndexFeatures.Builder().build();

    private StandardIndexInformation() {
    }

    @Override
    public boolean supports(KeyInformation information, HugeGraphPredicate hugegraphPredicate) {
        return hugegraphPredicate == Cmp.EQUAL || hugegraphPredicate == Contain.IN;
    }

    @Override
    public boolean supports(KeyInformation information) {
        return true;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        return key;
    }

    @Override
    public IndexFeatures getFeatures() {
        return STANDARD_FEATURES;
    }
}
