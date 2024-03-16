package com.integration.em.cluster;

import com.integration.em.model.Triple;

import java.util.*;

public class ConnectComponentCluster<T> extends GraphBasedCluster<T>{

    HashMap<T, Set<T>> clusterAssignment = new HashMap<>();

    @Override
    public Map<Collection<T>, T> cluster(Collection<Triple<T, T, Double>> similarityGraph) {
        clusterAssignment = new HashMap<>();

        for(Triple<T, T, Double> edge : similarityGraph) {

            addEdge(edge);

        }

        return createResult();
    }

    public void addEdge(Triple<T, T, Double> edge) {
        Set<T> first = clusterAssignment.get(edge.getFirst());
        Set<T> second = clusterAssignment.get(edge.getSecond());

        if(first==null && second==null) {

            Set<T> clu = new HashSet<>();
            clu.add(edge.getFirst());
            clu.add(edge.getSecond());

            clusterAssignment.put(edge.getFirst(), clu);
            clusterAssignment.put(edge.getSecond(), clu);
        } else if(first!=null && second==null) {
            first.add(edge.getSecond());

            clusterAssignment.put(edge.getSecond(), first);
        } else if(first==null && second!=null) {
            second.add(edge.getFirst());

            clusterAssignment.put(edge.getFirst(), second);
        } else {
            if(!first.equals(second)) {
                for(T node : second) {
                    clusterAssignment.put(node, first);
                }
                first.addAll(second);
            }
        }
    }

    public boolean isEdgeAlreadyInCluster(T firstNode, T secondNode) {
        Set<T> first = clusterAssignment.get(firstNode);
        Set<T> second = clusterAssignment.get(secondNode);

        return first!=null && second!=null && first.equals(second);
    }

    public Map<Collection<T>, T> createResult() {
        Map<Collection<T>, T> result = new HashMap<>();
        for (Collection<T> cluster : clusterAssignment.values()) {
            result.put(cluster, null);
        }
        return result;
    }
}
