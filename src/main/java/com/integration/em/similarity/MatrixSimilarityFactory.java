package com.integration.em.similarity;

public abstract class MatrixSimilarityFactory {
    public abstract <T> MatrixSimilarity<T> createMatrixSimilarity(int first, int second);

}
