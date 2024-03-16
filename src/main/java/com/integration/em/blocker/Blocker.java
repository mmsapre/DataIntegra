package com.integration.em.blocker;

import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.processing.Processable;

public interface Blocker<RecordType extends Matchable, SchemaElementType extends Matchable, BlockedType extends Matchable, AlignerType extends Matchable> {


    Processable<Aligner<BlockedType, AlignerType>> createBlocking(DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
            Processable<Aligner<AlignerType, Matchable>> schemaAligner);

    double getReductionRatio();

    boolean isMeasureBlockSizes();

    void writeDebugBlockingResultsToFile();


}
