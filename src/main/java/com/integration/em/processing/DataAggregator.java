package com.integration.em.processing;

import com.integration.em.tables.Pair;

import java.io.Serializable;
public interface DataAggregator<KeyType, RecordType, ResultType> extends Serializable {

    Pair<ResultType,Object> initialise(KeyType keyValue);

    Pair<ResultType,Object> aggregate(ResultType previousResult, RecordType record, Object state);

    Pair<ResultType,Object> merge(Pair<ResultType, Object> intermediateResult1, Pair<ResultType, Object> intermediateResult2);

    default ResultType createFinalValue(KeyType keyValue, ResultType result, Object state) {
        return result;
    }

    default Pair<ResultType, Object> stateless(ResultType result) {
        return new Pair<>(result,null);
    }

    default Pair<ResultType, Object> state(ResultType result, Object state) {
        return new Pair<>(result,state);
    }

}
