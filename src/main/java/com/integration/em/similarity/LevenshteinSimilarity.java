package com.integration.em.similarity;

import com.wcohen.ss.Levenstein;

public class LevenshteinSimilarity extends Similarity<String>{
    @Override
    public double calculate(String first, String second) {
        if (first == null || second == null) {
            return 0.0;
        } else {
            Levenstein l = new Levenstein();

            double score = Math.abs(l.score(first, second));
            score = score / Math.max(first.length(), second.length());

            return 1 - score;
        }
    }
}
