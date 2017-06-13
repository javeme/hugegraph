/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.base;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

import com.baidu.hugegraph.HugeGraph;

/**
 * Created by zhangsuochao on 17/5/24.
 */
@RunWith(HugeProcessBasicSuite.class)
@GraphProviderClass(provider = HugeGraphProvider.class, graph = HugeGraph.class)
public class HugeGraphProcessStandardTest {
}
