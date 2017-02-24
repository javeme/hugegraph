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

package com.baidu.hugegraph.diskstorage.es;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.core.HugeGraphFactory;

public class ElasticSearchConstants {

    public static final String ES_PROPERTIES_FILE = "hugegraph-es.properties";
    public static final String ES_VERSION_EXPECTED;

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchConstants.class);

    static {
        Properties props;

        try {
            props = new Properties();
            props.load(HugeGraphFactory.class.getClassLoader().getResourceAsStream(ES_PROPERTIES_FILE));
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        ES_VERSION_EXPECTED = props.getProperty("es.version");
    }
}
