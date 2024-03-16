package com.integration.em.matches;

import com.integration.em.similarity.MatrixSimilarityFactory;
import com.integration.em.similarity.SparseMatrixSimilarityFactory;

public abstract class MatrixMatching {

    private MatrixSimilarityFactory matrixSimilarityFactory;

    public MatrixMatching() {
       setMatrixSimilarityFactory(new SparseMatrixSimilarityFactory());
    }

    public MatrixSimilarityFactory getMatrixSimilarityFactory(){
        return matrixSimilarityFactory;
    }

    public void setMatrixSimilarityFactory(MatrixSimilarityFactory matrixSimilarityFactory) {
        this.matrixSimilarityFactory = matrixSimilarityFactory;
    }
}
