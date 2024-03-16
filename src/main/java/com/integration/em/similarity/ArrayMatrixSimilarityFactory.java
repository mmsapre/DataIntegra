package com.integration.em.similarity;

public class ArrayMatrixSimilarityFactory extends MatrixSimilarityFactory{
    @Override
    public <T> MatrixSimilarity<T> createMatrixSimilarity(int first, int second) {
        return new ArrayMatrixSimilarity<T>(first, second);
    }
}
