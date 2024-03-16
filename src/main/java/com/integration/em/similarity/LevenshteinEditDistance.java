package com.integration.em.similarity;

import com.wcohen.ss.Levenstein;

public class LevenshteinEditDistance extends Similarity<String>{
    @Override
    public double calculate(String first, String second) {
        if (first == null || second == null) {
            return -1.0;
        } else {
            Levenstein l = new Levenstein();
            return Math.abs(l.score(first, second));
        }
    }
}
