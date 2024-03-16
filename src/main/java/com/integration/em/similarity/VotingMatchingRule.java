package com.integration.em.similarity;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.DataIterator;
import com.integration.em.processing.Processable;
import com.integration.em.processing.ProcessableCollection;
import com.integration.em.rules.AggregableMatchingRule;

public abstract class VotingMatchingRule<RecordType extends Matchable, SchemaElementType extends Matchable>
        extends AggregableMatchingRule<RecordType, SchemaElementType> {

    public VotingMatchingRule(double finalThreshold) {
        super(finalThreshold);
    }


    @Override
    public void mapRecord(Aligner<RecordType, SchemaElementType> record,
                          DataIterator<Aligner<RecordType, SchemaElementType>> resultCollector) {

        for(Aligner<SchemaElementType, Matchable> alg : record.getCausalAligners().get()) {
            Aligner<RecordType, SchemaElementType> newCor = apply(record.getFirstRecordType(), record.getSecondRecordType(), alg);

            if(newCor!=null) {
                resultCollector.next(newCor);
            }
        }
    }

    public Aligner<RecordType, SchemaElementType> apply(RecordType record1,
                                                               RecordType record2, Aligner<SchemaElementType, Matchable> aligners) {
        double sim = compare(record1, record2, aligners);

        Processable<Aligner<SchemaElementType, Matchable>> cause = new ProcessableCollection<>();
        cause.add(aligners);
        return new Aligner<RecordType, SchemaElementType>(record1, record2, sim, cause);
    }


    @Override
    public abstract double compare(RecordType record1, RecordType record2, Aligner<SchemaElementType, Matchable> aligner);

}
