package com.integration.em.similarity;

public class EqualsSimilarity<DataType> extends Similarity<DataType>{

    @Override
    public double calculate(DataType first, DataType second) {
        if (first == null || second == null) {
            return 0.0;
        } else {
            return first.equals(second) ? 1.0 : 0.0;
        }
    }
}
