/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

import com.baidu.hugegraph.HugeGraphProvider;

/**
 * Created by zhangsuochao on 17/2/13.
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = HugeGraphProvider.class, graph = HugeGraph.class)
public class HugeGraphStructureStandardTest {
}
