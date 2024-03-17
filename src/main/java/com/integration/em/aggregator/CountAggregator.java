package com.integration.em.aggregator;

import com.integration.em.processing.DataAggregator;
import com.integration.em.tables.Pair;

public class CountAggregator<KeyType, RecordType> implements DataAggregator<KeyType, RecordType, Integer> {

    @Override
    public Pair<Integer,Object> aggregate(Integer previousResult,
                                          RecordType record, Object state) {
        if(previousResult==null) {
            return stateless(1);
        } else {
            return stateless(previousResult+1);
        }
    }

    @Override
    public Pair<Integer, Object> merge(Pair<Integer, Object> intermediateResult1,
                                       Pair<Integer, Object> intermediateResult2) {
        return stateless(intermediateResult1.getFirst()+intermediateResult2.getFirst());
    }

    @Override
    public Pair<Integer, Object> initialise(KeyType keyValue) {
        return stateless(null);
    }
}
