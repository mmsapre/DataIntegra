package com.integration.em.similarity;

import java.util.Map;

public interface VectorSpaceSimilarity {

    double calculateDimensionScore(double vector1, double vector2);

    double aggregateDimensionScores(double lastScore, double nextScore);

    double normaliseScore(double score, Map<String, Double> vector1, Map<String, Double> vector2);
}
