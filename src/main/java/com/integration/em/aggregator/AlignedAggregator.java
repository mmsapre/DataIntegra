package com.integration.em.aggregator;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.DataAggregator;
import com.integration.em.tables.Pair;

public class AlignedAggregator<TypeA extends Matchable, TypeB extends Matchable>
        implements DataAggregator<Pair<TypeA, TypeA>, Aligner<TypeA,TypeB>, Aligner<TypeA,TypeB>> {

    private double finalThreshold = 0.0;

    public AlignedAggregator(double finalThreshold) {
        this.finalThreshold = finalThreshold;
    }

    public double getFinalThreshold() {
        return finalThreshold;
    }


    protected double getSimilarityScore(Aligner<TypeA, TypeB> aligner) {
        return aligner.getSimilarityScore();
    }
    @Override
    public Pair<Aligner<TypeA, TypeB>, Object> initialise(Pair<TypeA, TypeA> keyValue) {
        return stateless(null);
    }

    @Override
    public Pair<Aligner<TypeA, TypeB>, Object> aggregate(Aligner<TypeA, TypeB> previousResult, Aligner<TypeA, TypeB> aligner, Object state) {
        if(previousResult==null) {
            aligner.setSimilarityScore(getSimilarityScore(aligner));
            return stateless(aligner);
        } else {
            previousResult.setSimilarityScore(previousResult.getSimilarityScore() + getSimilarityScore(aligner));

            if(aligner.getCausalAligners()!=null) {

                if(previousResult.getCausalAligners()!=null) {
                    previousResult.setCausalAligners(previousResult.getCausalAligners().append(aligner.getCausalAligners()));
                } else {
                    previousResult.setCausalAligners(aligner.getCausalAligners().copy());
                }
            }
            return stateless(previousResult);
        }
    }

    @Override
    public Aligner<TypeA, TypeB> createFinalValue(Pair<TypeA, TypeA> keyValue,
                                                         Aligner<TypeA, TypeB> result, Object state) {

        if(result.getSimilarityScore()>0.0 && result.getSimilarityScore()>=getFinalThreshold()) {
            return result;
        } else {
            return null;
        }
    }

    @Override
    public Pair<Aligner<TypeA, TypeB>, Object> merge(Pair<Aligner<TypeA, TypeB>, Object> intermediateResult1, Pair<Aligner<TypeA, TypeB>, Object> intermediateResult2) {
        return aggregate(intermediateResult1.getFirst(), intermediateResult2.getFirst(), null);
    }
}
