package com.integration.em.processing;

import com.integration.em.tables.Pair;

public class StringConcatAggregator<KeyType> implements DataAggregator<KeyType, String, String> {

    private String separator;

    public StringConcatAggregator(String separator) {
        this.separator = separator;
    }

    @Override
    public Pair<String, Object> initialise(KeyType keyValue) {
        return stateless(null);
    }

    @Override
    public Pair<String, Object> aggregate(String previousResult, String record, Object state) {
        if(previousResult==null) {
            return stateless(record);
        } else {
            return stateless(previousResult + separator + record);
        }
    }

    @Override
    public Pair<String, Object> merge(Pair<String, Object> intermediateResult1, Pair<String, Object> intermediateResult2) {
        return aggregate(intermediateResult1.getFirst(), intermediateResult2.getFirst(), null);    }
}
