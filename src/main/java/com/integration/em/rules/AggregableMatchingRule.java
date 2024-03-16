package com.integration.em.rules;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.DataIterator;
import com.integration.em.processing.ProcessableCollection;
import com.integration.em.processing.ProcessableCollector;
import com.integration.em.processing.RecordKeyValueMapper;
import com.integration.em.tables.Pair;

public abstract class AggregableMatchingRule<RecordType extends Matchable, SchemaElementType extends Matchable> extends MatchingRule<RecordType, SchemaElementType>
        implements RecordKeyValueMapper<Pair<RecordType, RecordType>, Aligner<RecordType, SchemaElementType>, Aligner<RecordType, SchemaElementType>> {

    public AggregableMatchingRule(double finalThreshold) {
        super(finalThreshold);
    }

    @Override
    public void mapRecordToKey(Aligner<RecordType, SchemaElementType> record, DataIterator<Pair<Pair<RecordType, RecordType>, Aligner<RecordType, SchemaElementType>>> resultCollector) {

        ProcessableCollector<Aligner<RecordType, SchemaElementType>> collector = new ProcessableCollector<>();

        collector.setResult(new ProcessableCollection<>());
        collector.initialise();

        mapRecord(record, collector);

        collector.finalise();

        for(Aligner<RecordType, SchemaElementType> aligner : collector.getResult().get()) {
            if(aligner.getSimilarityScore()<getFinalThreshold()) {

                aligner = new Aligner<>(aligner.getFirstRecordType(), aligner.getSecondRecordType(), 0.0, aligner.getCausalAligners());
            }

            resultCollector.next(new Pair<>(generateAggregationKey(aligner), aligner));
        }
    }

    protected Pair<RecordType, RecordType> generateAggregationKey(Aligner<RecordType, SchemaElementType> aligner) {
        return new Pair<RecordType, RecordType>(aligner.getFirstRecordType(), aligner.getSecondRecordType());
    }

}
