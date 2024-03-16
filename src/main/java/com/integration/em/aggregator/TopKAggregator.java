package com.integration.em.aggregator;
import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.DataAggregator;
import com.integration.em.processing.Processable;
import com.integration.em.processing.ProcessableCollection;
import com.integration.em.tables.Pair;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopKAggregator<TypeA extends Matchable, TypeB extends Matchable, KeyType>
        implements DataAggregator<KeyType, Aligner<TypeA,TypeB>, Processable<Aligner<TypeA,TypeB>>> {

    private int k;

    public TopKAggregator(int k) {
        this.k = k;
    }

    @Override
    public Pair<Processable<Aligner<TypeA, TypeB>>, Object> initialise(KeyType keyValue) {
        return stateless(new ProcessableCollection<>());
    }

    @Override
    public Pair<Processable<Aligner<TypeA, TypeB>>, Object> aggregate(Processable<Aligner<TypeA, TypeB>> previousResult,
                                                                             Aligner<TypeA, TypeB> record, Object state) {

        previousResult.add(record);

        if(k>0) {

            previousResult = previousResult.sort((r)->r.getIdentifiers()).sort((r)->r.getSimilarityScore(), false).take(k);
        }

        return stateless(previousResult);
    }

    @Override
    public Pair<Processable<Aligner<TypeA, TypeB>>, Object> merge(
            Pair<Processable<Aligner<TypeA, TypeB>>, Object> intermediateResult1,
            Pair<Processable<Aligner<TypeA, TypeB>>, Object> intermediateResult2) {
        return stateless(intermediateResult1.getFirst().append(intermediateResult2.getFirst()).sort((r)->r.getIdentifiers()).sort((r)->r.getSimilarityScore(), false).take(k));
    }
}
