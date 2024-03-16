package com.integration.em.rules;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.DataIterator;

public class IdentifierMatchingRule<TypeA extends Matchable, TypeB extends Matchable>
        extends AggregableMatchingRule<TypeA, TypeB> {

    public IdentifierMatchingRule(double finalThreshold) {
        super(finalThreshold);
    }

    @Override
    public void mapRecord(Aligner<TypeA, TypeB> record, DataIterator<Aligner<TypeA, TypeB>> resultCollector) {
        resultCollector.next(record);
    }

    @Override
    public double compare(TypeA record1, TypeA record2, Aligner<TypeB, Matchable> schemaAligner) {
        return 0;
    }
}
