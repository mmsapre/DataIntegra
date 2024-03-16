package com.integration.em.similarity;

import com.integration.em.utils.Q;

import java.util.Map;
import java.util.Set;

public class VectorJaccardSimilarity implements VectorSpaceSimilarity{
    @Override
    public double calculateDimensionScore(double vector1, double vector2) {
        return Math.min(vector1, vector2);
    }

    @Override
    public double aggregateDimensionScores(double lastScore, double nextScore) {
        return lastScore + nextScore;
    }

    @Override
    public double normaliseScore(double score, Map<String, Double> vector1, Map<String, Double> vector2) {
        Set<String> allDimensions = Q.union(vector1.keySet(), vector2.keySet());

        double normaliseWith = 0.0;

        for(String dimension : allDimensions) {
            Double score1 = vector1.get(dimension);
            Double score2 = vector2.get(dimension);

            if(score1==null) score1 = 0.0;
            if(score2==null) score2 = 0.0;

            normaliseWith += Math.max(score1, score2);

        }

        return score / normaliseWith;
    }
}
