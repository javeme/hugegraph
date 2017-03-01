package com.baidu.hugegraph.hadoop.formats.graphson

def hugegraphVertex getOrCreateVertex(FaunusVertex faunusVertex, hugegraph graph, TaskInputOutputContext context, Logger log) {
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

def hugegraphEdge getOrCreateEdge(FaunusEdge faunusEdge, hugegraphVertex inVertex, hugegraphVertex outVertex, hugegraph graph, TaskInputOutputContext context, Logger log) {
    final String label = faunusEdge.label();

    log.debug("outVertex:{} label:{} inVertex:{}", outVertex, label, inVertex);

    final Edge hugegraphEdge = !outVertex.out(label).has("id", inVertex.id()).hasNext() ?
        graph.addEdge(null, outVertex, inVertex, label) :
        outVertex.outE(label).as("here").inV().has("id", inVertex.id()).back("here").next();

    return hugegraphEdge;
}

def void getOrCreateVertexProperty(hugegraphProperty faunusProperty, hugegraphVertex vertex, hugegraph graph, TaskInputOutputContext context, Logger log) {

    final com.baidu.hugegraph.core.PropertyKey pkey = faunusProperty.propertyKey();
    if (pkey.cardinality().equals(com.baidu.hugegraph.core.Cardinality.SINGLE)) {
        vertex.property(pkey.name(), faunusProperty.value());
    } else {
//        Iterator<com.baidu.hugegraph.core.hugegraphProperty> itty = vertex.getProperties(pkey.getName()).iterator();
//        if (!itty.hasNext()) {
            vertex.property(pkey.name(), faunusProperty.value());
//        }
    }

    log.debug("Set property {}={} on {}", pkey.name(), faunusProperty.value(), vertex);
}
