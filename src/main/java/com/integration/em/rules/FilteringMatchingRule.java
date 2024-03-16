package com.integration.em.rules;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.DataIterator;
import com.integration.em.processing.Processable;

public abstract class FilteringMatchingRule<RecordType extends Matchable, SchemaElementType extends Matchable>
        extends MatchingRule<RecordType, SchemaElementType> {

    public FilteringMatchingRule(double finalThreshold) {
        super(finalThreshold);
    }

    @Override
    public void mapRecord(Aligner<RecordType, SchemaElementType> record, DataIterator<Aligner<RecordType, SchemaElementType>> resultCollector) {

        Aligner<RecordType, SchemaElementType> cor = apply(record.getFirstRecordType(), record.getSecondRecordType(),
                record.getCausalAligners());

        if (cor != null && cor.getSimilarityScore() > 0.0 && cor.getSimilarityScore() >= getFinalThreshold()) {
            resultCollector.next(cor);
        }
    }



    public abstract Aligner<RecordType, SchemaElementType> apply(RecordType record1, RecordType record2,
                                                                        Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable);
}
