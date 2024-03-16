package com.integration.em.similarity;

import java.util.ArrayList;
import java.util.Collection;

public class ArrayMatrixSimilarity<T> extends MatrixSimilarity<T>{

    private ArrayList<T> firstDim;
    private ArrayList<T> secondDim;
    private Double[][] similarities;

    public ArrayMatrixSimilarity(int fistDimSize,int secondDimSize) {

        firstDim = new ArrayList<T>(fistDimSize);
        secondDim = new ArrayList<T>(secondDimSize);
        similarities = new Double[fistDimSize][secondDimSize];
    }


    @Override
    public Double get(T first, T second) {
        int idx1 = firstDim.indexOf(first);
        int idx2 = secondDim.indexOf(second);

        if (idx1 != -1 && idx2 != -1) {
            return similarities[idx1][idx2];
        } else {
            return null;
        }
    }

    @Override
    public void set(T first, T second, Double similarity) {

        int idx1 = -1, idx2 = 1;
        idx1 = firstDim.indexOf(first);
        if (idx1 == -1) {
            synchronized (firstDim) {
                firstDim.add(first);
                idx1 = firstDim.size() - 1;
            }
        }
        idx2 = secondDim.indexOf(second);
        if (idx2 == -1) {
            synchronized (secondDim) {
                secondDim.add(second);
                idx2 = secondDim.size() - 1;
            }
        }
        similarities[idx1][idx2] = similarity;
    }

    @Override
    public Collection<T> getFirstDim() {
        return firstDim;
    }

    @Override
    public Collection<T> getSecondDim() {
        return secondDim;
    }

    @Override
    public Collection<T> getMatches(T first) {
        return getMatchesAboveThreshold(first, Double.NEGATIVE_INFINITY);
    }

    @Override
    public Collection<T> getMatchesAboveThreshold(T first, double similarityThreshold) {
        int idx1 = firstDim.indexOf(first);
        if (idx1 == -1) {
            return null;
        } else {
            Collection<T> result = new ArrayList<T>(secondDim.size());
            for (int i = 0; i < secondDim.size(); i++) {
                if (similarities[idx1][i]!=null && similarities[idx1][i] > similarityThreshold) {
                    result.add(secondDim.get(i));
                }
            }
            return result;
        }
    }

    @Override
    protected MatrixSimilarity<T> createEmptyCopy() {
        return null;
    }
}
