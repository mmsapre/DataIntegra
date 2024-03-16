package com.integration.em.processing;

import com.integration.em.tables.Pair;
import com.integration.em.utils.Distribution;
import com.integration.em.utils.Q;
public abstract class DistributionAggregator<KeyType, RecordType, InnerKeyType> implements DataAggregator<KeyType, RecordType, Distribution<InnerKeyType>> {

    @Override
    public Pair<Distribution<InnerKeyType>,Object> initialise(KeyType keyValue) {
        return stateless(new Distribution<>());
    }

    @Override
    public Pair<Distribution<InnerKeyType>,Object> aggregate(Distribution<InnerKeyType> previousResult, RecordType record, Object state) {

        previousResult.add(getInnerKey(record));

        return stateless(previousResult);
    }

    @Override
    public Pair<Distribution<InnerKeyType>, Object> merge(Pair<Distribution<InnerKeyType>, Object> intermediateResult1,
                                                          Pair<Distribution<InnerKeyType>, Object> intermediateResult2) {

        Distribution<InnerKeyType> dist1 = intermediateResult1.getFirst();
        Distribution<InnerKeyType> dist2 = intermediateResult2.getFirst();

        Distribution<InnerKeyType> result = new Distribution<>();

        for(InnerKeyType elem : Q.union(dist1.getElements(), dist2.getElements())) {
            result.add(elem, dist1.getFrequency(elem));
            result.add(elem, dist2.getFrequency(elem));
        }

        return stateless(result);
    }

    public abstract InnerKeyType getInnerKey(RecordType record);

}
