package com.integration.em.aggregator;

import com.integration.em.model.Matchable;
import com.integration.em.tables.Pair;

public class TopKVotesAggregator<TypeA extends Matchable, TypeB extends Matchable> extends TopKAggregator<TypeA, TypeB, Pair<TypeA, TypeB>> {

    public TopKVotesAggregator(int k) {
        super(k);
    }

}
