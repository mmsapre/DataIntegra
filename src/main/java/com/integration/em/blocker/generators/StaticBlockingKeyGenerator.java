package com.integration.em.blocker.generators;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.DataIterator;
import com.integration.em.processing.Processable;
import com.integration.em.tables.Pair;

public class StaticBlockingKeyGenerator<RecordType extends Matchable, AlignerType extends Matchable> extends
        BlockingKeyGenerator<RecordType, AlignerType, RecordType> {


    private static final String STATIC_BLOCKING_KEY = "AAA";


    @Override
    public void generateBlockingKeys(RecordType record, Processable<Aligner<AlignerType, Matchable>> correspondences, DataIterator<Pair<String, RecordType>> resultCollector) {

        resultCollector.next(new Pair<>(STATIC_BLOCKING_KEY, record));

    }
}
