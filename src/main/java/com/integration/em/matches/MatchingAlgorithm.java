package com.integration.em.matches;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.Processable;

public interface MatchingAlgorithm<TypeA extends Matchable, TypeB extends Matchable> {

    void run();

    Processable<Aligner<TypeA, TypeB>> getResult();
}
