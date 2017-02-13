/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

/**
 * Created by zhangsuochao on 17/2/8.
 */
public class HugeGraphTest {

    @Test
    public void testAddVertex(){
        HugeGraph g = HugeGraph.open();
        for(int i=20;i<30;i++){
            g.addVertex(T.id,i,T.label,"Book","name","book"+i,"pages",20+i);
        }

    }

//    @Test
//    public void testQueryVertex(){
//        HugeGraph g = HugeGraph.open();
//        Iterator<Vertex> it = g.vertices();
//        while (it.hasNext()){
//            HugeVertex v = (HugeVertex)it.next();
//            Map<String,Object> properties = v.getProperties();
//            System.out.println("id:"+v.id);
//            System.out.println("label:"+v.label);
//            for (String k:properties.keySet()){
//                System.out.println(k+":"+properties.get(k));
//            }
//
//        }
//    }
}
