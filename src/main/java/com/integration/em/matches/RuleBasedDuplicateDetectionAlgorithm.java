package com.integration.em.matches;

import com.integration.em.blocker.SymmetricBlocker;
import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.processing.Processable;
import com.integration.em.rules.MatchingRule;

public class RuleBasedDuplicateDetectionAlgorithm<TypeA extends Matchable, TypeB extends Matchable> extends RuleBasedMatchingAlgorithm<TypeA, TypeB, TypeB> {


    private SymmetricBlocker<TypeA, TypeB, TypeA, TypeB> blocker;

    public RuleBasedDuplicateDetectionAlgorithm(DataSet<TypeA, TypeB> dataset, MatchingRule<TypeA, TypeB> rule,
                                                SymmetricBlocker<TypeA, TypeB, TypeA, TypeB> blocker) {
        super(dataset, null, null, rule, null);
        this.blocker = blocker;
    }

    public RuleBasedDuplicateDetectionAlgorithm(DataSet<TypeA, TypeB> dataset, Processable<Aligner<TypeB, Matchable>> alignerProcessable, MatchingRule<TypeA, TypeB> rule,
                                                SymmetricBlocker<TypeA, TypeB, TypeA, TypeB> blocker) {
        super(dataset, null, alignerProcessable, rule, null);
        this.blocker = blocker;
    }

    @Override
    public Processable<Aligner<TypeA, TypeB>> createBlocking(DataSet<TypeA, TypeB> dataset1,
                                                                 DataSet<TypeA, TypeB> dataset2, Processable<Aligner<TypeB, Matchable>> alignerProcessable) {
        return blocker.createBlocking(dataset1, alignerProcessable);
    }

    @Override
    public double getReductionRatio() {
        return blocker.getReductionRatio();
    }

    @Override
    public void run() {
        super.run();

        Aligner.setDirectionByDataSourceIdentifier(getResult());
    }
}
