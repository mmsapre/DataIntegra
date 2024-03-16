package com.integration.em.similarity;

import java.util.Map;

public class VectorCossineSimilarity implements VectorSpaceSimilarity{
    @Override
    public double calculateDimensionScore(double vector1, double vector2) {
        return vector1 * vector2;
    }

    @Override
    public double aggregateDimensionScores(double lastScore, double nextScore) {
        return lastScore + nextScore;
    }

    @Override
    public double normaliseScore(double score, Map<String, Double> vector1, Map<String, Double> vector2) {
        double leftLength = 0;
        double rightLength = 0;

        for(Double d : vector1.values()) {
            leftLength += d * d;
        }
        for(Double d : vector2.values()) {
            rightLength += d * d;
        }

        return score / (Math.sqrt(leftLength) * Math.sqrt(rightLength));
    }
}
