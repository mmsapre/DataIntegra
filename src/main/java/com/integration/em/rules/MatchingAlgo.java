package com.integration.em.rules;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.Processable;

public interface MatchingAlgo<TypeA extends Matchable, TypeB extends Matchable> {

    void run();

    Processable<Aligner<TypeA, TypeB>> getResult();
}
