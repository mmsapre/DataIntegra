package com.integration.em.similarity;

public class SparseMatrixSimilarityFactory extends MatrixSimilarityFactory{
    @Override
    public <T> MatrixSimilarity<T> createMatrixSimilarity(int first, int second) {
        return new SparseMatrixSimilarity<T>(first,second);
    }
}
