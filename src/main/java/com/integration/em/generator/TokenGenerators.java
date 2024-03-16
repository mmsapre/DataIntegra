package com.integration.em.generator;

import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.DataIterator;
import com.integration.em.processing.Processable;
import com.integration.em.tables.Pair;

public abstract class TokenGenerators <RecordType extends Matchable, AlignerType extends Matchable> extends BlockingKeyGenerator<RecordType, AlignerType, RecordType> {

    @Override
    public void generateBlockingKeys(RecordType record, Processable<Aligner<AlignerType, Matchable>> alignerProcessable, DataIterator<Pair<String, RecordType>> resultCollector) {
        generateTokens(record, alignerProcessable, resultCollector);
    }

    public abstract void generateTokens(RecordType record, Processable<Aligner<AlignerType, Matchable>> alignerProcessable, DataIterator<Pair<String, RecordType>> resultCollector);

    public abstract String[] tokenizeString(String value);


}
