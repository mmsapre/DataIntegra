package com.integration.em.blocker;

import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.processing.Processable;

public interface SymmetricBlocker <RecordType extends Matchable, SchemaElementType extends Matchable, BlockedType extends Matchable, AlignerType extends Matchable>{

    public double getReductionRatio();

    public Processable<Aligner<BlockedType, AlignerType>> createBlocking(
            DataSet<RecordType, SchemaElementType> dataset,
            Processable<Aligner<AlignerType, Matchable>> alignerProcessable);
}
