package com.integration.em.similarity;

import java.util.*;

public class SparseMatrixSimilarity<T> extends MatrixSimilarity<T> {

    private Set<T> firstSet;
    private Set<T> secondSet;
    private Map<T, Map<T, Double>> sparseMartrix;

    protected int expectedFirstSize;
    protected int expectedSecondSize;

    public SparseMatrixSimilarity(int first,int second) {
        this.expectedFirstSize = first;
        this.expectedSecondSize = second;
        sparseMartrix=createOuterCells();
    }

    public Map<T, Map<T, Double>> getSparseMartrix() {
        return sparseMartrix;
    }

    protected Map<T, Map<T, Double>> createOuterCells() {
        return new HashMap<T, Map<T, Double>>(expectedFirstSize);
    }

    protected Map<T, Double> createInnerCells() {
        return new HashMap<T, Double>(expectedSecondSize);
    }
    @Override
    public Double get(T first, T second) {

        Map<T, Double> innerMap = getSparseMartrix().get(first);
        if (innerMap != null) {
            Double value = innerMap.get(second);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    @Override
    public void set(T first, T second, Double similarity) {

        if (first == null || second == null) {
            throw new NullPointerException();
        }

        Map<T, Double> innerMap = getSparseMartrix().get(first);

        if (innerMap == null) {
            innerMap = createInnerCells();

            getSparseMartrix().put(first, innerMap);

            if (first != null) {
                firstSet.add(first);
            }
        }

        if (secondSet != null && innerMap.get(second) == null) {
            secondSet.add(second);
        }

        if(similarity==null) {
            innerMap.remove(second);
        } else {
            innerMap.put(second, similarity);
        }
    }

    protected Set<T> createFirstCache() {
        return new HashSet<T>();
    }

    protected Set<T> createSecondCache() {
        return new HashSet<T>();
    }
    protected void cacheFirst() {
        firstSet = createFirstCache();

        firstSet.addAll(getSparseMartrix().keySet());
    }

    protected void cacheSecond() {
        secondSet = createSecondCache();

        for (Map<T, Double> map : getSparseMartrix().values()) {
            secondSet.addAll(map.keySet());
        }
    }
    @Override
    public Collection<T> getFirstDim() {
        if (firstSet == null) {
            synchronized (this) {
                cacheFirst();
            }
        }
        return firstSet;
    }

    @Override
    public Collection<T> getSecondDim() {
        if (secondSet == null) {
            synchronized (this) {
                cacheSecond();
            }
        }

        return secondSet;
    }

    @Override
    public Collection<T> getMatches(T first) {
        return getMatchesAboveThreshold(first, Double.NEGATIVE_INFINITY);
    }

    @Override
    public Collection<T> getMatchesAboveThreshold(T first, double similarityThreshold) {
        Map<T, Double> innerMap = getSparseMartrix().get(first);

        if (innerMap != null) {
            Collection<T> result = new ArrayList<T>(innerMap.keySet().size());

            for (T second : innerMap.keySet()) {
                if (innerMap.get(second) != null
                        && innerMap.get(second) > similarityThreshold) {
                    result.add(second);
                }
            }

            return result;
        } else {
            return new ArrayList<T>();
        }
    }

    @Override
    protected MatrixSimilarity<T> createEmptyCopy() {
        return new SparseMatrixSimilarityFactory().createMatrixSimilarity(getFirstDim().size(), getSecondDim().size());
    }
}
