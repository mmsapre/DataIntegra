package com.integration.em.processing;


import com.integration.em.tables.Pair;

import java.util.HashMap;
import java.util.Map;

public class AggregateCollector<KeyType, RecordType, ResultType> extends GroupCollector<KeyType, RecordType> {

    private Map<KeyType, Pair<ResultType,Object>> intermediateResults;
    private Processable<Pair<KeyType, ResultType>> aggregationResult;
    private DataAggregator<KeyType, RecordType, ResultType> aggregator;

    @Override
    public void initialise() {
        super.initialise();
        intermediateResults = new HashMap<>();
        aggregationResult = new ProcessableCollection<>();
    }


    protected void setAggregationResult(
            Processable<Pair<KeyType, ResultType>> aggregationResult) {
        this.aggregationResult = aggregationResult;
    }

    public Processable<Pair<KeyType, ResultType>> getAggregationResult() {
        return aggregationResult;
    }


    public void setAggregator(DataAggregator<KeyType, RecordType, ResultType> aggregator) {
        this.aggregator = aggregator;
    }


    @Override
    public void next(Pair<KeyType, RecordType> record) {
        Pair<ResultType,Object> result = intermediateResults.get(record.getFirst());

        if(result==null) {
            result = aggregator.initialise(record.getFirst());
        }

        result = aggregator.aggregate(result.getFirst(), record.getSecond(), result.getSecond());
        intermediateResults.put(record.getFirst(), result);
    }


    @Override
    public void finalise() {
        for(KeyType key : intermediateResults.keySet()) {
            Pair<ResultType, Object> value = intermediateResults.get(key);
            ResultType result = aggregator.createFinalValue(key, value.getFirst(), value.getSecond());
            if(result!=null) {
                aggregationResult.add(new Pair<>(key, result));
            }
        }
    }
}
