package com.integration.em.similarity;

import com.integration.em.matches.BestMatch;

public class GLJaccard<T extends Comparable<? super T>> extends ADTSimilarity<T> {

    public GLJaccard(Similarity<T> similarity, double tSimilarity) {

        setSimilarity(similarity);
        setSimilarityThreshold(tSimilarity);
    }

    @Override
    protected Double aggregateSimilarity(MatrixSimilarity<T> matrix) {

        double firstLength = matrix.getFirstDim().size();
        double secondLength = matrix.getSecondDim().size();

        BestMatch bestMatch = new BestMatch();
        bestMatch.setForceOneToOneMapping(true);
        matrix = bestMatch.match(matrix);

        double fuzzyMatching = matrix.getSum();

        return fuzzyMatching / (firstLength + secondLength - fuzzyMatching);
    }
}
