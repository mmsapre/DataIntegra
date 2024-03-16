package com.integration.em.processing;

import com.integration.em.tables.Pair;
import com.integration.em.utils.Function;

public class PairFirstJoinKeyGenerator<T, U> implements Function<T, Pair<T, U>> {
    @Override
    public T execute(Pair<T, U> input) {
        return input.getFirst();
    }
}
