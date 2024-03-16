package com.integration.em.aggregator;

import com.integration.em.processing.DataAggregator;
import com.integration.em.tables.Pair;

public abstract class SumAggregator<KeyType, RecordType> implements DataAggregator<KeyType, RecordType, Double> {
    @Override
    public Pair<Double, Object> initialise(KeyType keyValue) {
        return stateless(null);
    }

    @Override
    public Pair<Double, Object> aggregate(Double previousResult, RecordType record, Object state) {
        if(previousResult==null) {
            return stateless(getValue(record));
        } else {
            return stateless(previousResult+getValue(record));
        }
    }

    @Override
    public Pair<Double, Object> merge(Pair<Double, Object> intermediateResult1, Pair<Double, Object> intermediateResult2) {
        return stateless(intermediateResult1.getFirst()+intermediateResult2.getFirst());
    }

    public abstract Double getValue(RecordType record);

}
