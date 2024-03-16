package com.integration.em.similarity;

import java.util.Collection;

public abstract class ADTSimilarity<T> extends Similarity<Collection<T>> {

    private  Similarity<T> similarity;
    private double similarityThreshold;

    protected abstract Double aggregateSimilarity(MatrixSimilarity<T> matrix);

    public Similarity<T> getSimilarity() {
        return similarity;
    }

    public void setSimilarity(Similarity<T> similarity) {
        this.similarity = similarity;
    }

    public double getSimilarityThreshold() {
        return similarityThreshold;
    }

    public void setSimilarityThreshold(double similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
    }

    @Override
    public double calculate(Collection<T> first, Collection<T> second) {

        MatrixSimilarity<T> matrix = new SparseMatrixSimilarityFactory().createMatrixSimilarity(first.size(), second.size());

        for(T t1 : first) {
            for(T t2 : second) {
                double sim = getSimilarity().calculate(t1, t2);
                if(sim >= getSimilarityThreshold()) {
                    matrix.set(t1, t2, sim);
                } else {
                    matrix.set(t1, t2, 0.0);
                }
            }
        }

        return aggregateSimilarity(matrix);
    }

}
