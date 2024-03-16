package com.integration.em.cluster;

import com.integration.em.model.Triple;

import java.util.Collection;
import java.util.Map;

public abstract class GraphBasedCluster<T> {

    public abstract Map<Collection<T>, T> cluster(Collection<Triple<T, T, Double>> similarityGraph);
}
