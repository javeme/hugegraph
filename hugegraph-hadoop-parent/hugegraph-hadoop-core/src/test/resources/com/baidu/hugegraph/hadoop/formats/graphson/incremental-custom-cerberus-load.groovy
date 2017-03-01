package com.baidu.hugegraph.hadoop.formats.graphson

import com.baidu.hugegraph.core.hugegraphVertexProperty

def hugegraphVertex getOrCreateVertex(faunusVertex, graph, context, log) {
    String uniqueKey = "name";
    Object uniqueValue = faunusVertex.value(uniqueKey);
    Vertex hugegraphVertex;
    if (null == uniqueValue)
      throw new RuntimeException("The provided Faunus vertex does not have a property for the unique key: " + faunusVertex);
  
    Iterator<Vertex> itty = graph.query().has(uniqueKey, uniqueValue).vertices().iterator();
    if (itty.hasNext()) {
      hugegraphVertex = itty.next();
      if (itty.hasNext())
        log.info("The unique key is not unique as more than one vertex with the value {}", uniqueValue);
    } else {
      hugegraphVertex = graph.addVertex(faunusVertex.longId(),faunusVertex.label());
    }
    return hugegraphVertex;
}

def void getOrCreateVertexProperty(faunusProperty, vertex, graph, context, log) {

    final com.baidu.hugegraph.core.PropertyKey pkey = faunusProperty.propertyKey();
    if (pkey.cardinality().equals(com.baidu.hugegraph.core.Cardinality.SINGLE)) {
        vertex.property(pkey.name(), faunusProperty.value());
    } else {
        Iterator<hugegraphVertexProperty> itty = vertex.getProperties(pkey.name()).iterator();
        if (!itty.hasNext()) {
            vertex.property(pkey.name(), faunusProperty.value());
        }
    }

    log.debug("Set property {}={} on {}", pkey.name(), faunusProperty.value(), vertex);
}
