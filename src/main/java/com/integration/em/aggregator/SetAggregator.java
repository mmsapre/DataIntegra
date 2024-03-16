package com.integration.em.aggregator;

import com.integration.em.processing.DataAggregator;
import com.integration.em.tables.Pair;

import java.util.HashSet;
import java.util.Set;

public class SetAggregator<KeyType, RecordType> implements DataAggregator<KeyType, RecordType, Set<RecordType>> {


    @Override
    public Pair<Set<RecordType>, Object> initialise(KeyType keyValue) {
        return stateless(new HashSet<>());
    }

    @Override
    public Pair<Set<RecordType>, Object> aggregate(Set<RecordType> previousResult, RecordType record, Object state) {
        previousResult.add(record);

        return stateless(previousResult);
    }

    @Override
    public Pair<Set<RecordType>, Object> merge(Pair<Set<RecordType>, Object> intermediateResult1, Pair<Set<RecordType>, Object> intermediateResult2) {

        Set<RecordType> result = intermediateResult1.getFirst();
        result.addAll(intermediateResult2.getFirst());

        return stateless(result);
    }
}
