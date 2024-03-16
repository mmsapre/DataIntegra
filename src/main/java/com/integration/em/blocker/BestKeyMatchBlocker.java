package com.integration.em.blocker;

import com.integration.em.model.Aligner;
import com.integration.em.model.BestMatchStandard;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.processing.ParallelProcessableCollection;
import com.integration.em.processing.Processable;
import com.integration.em.tables.Pair;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BestKeyMatchBlocker<RecordType extends Matchable, SchemaElementType extends Matchable, AlignerType extends Matchable>
        extends AbstractBlocker<RecordType, RecordType, AlignerType>
        implements Blocker<RecordType, SchemaElementType, RecordType, AlignerType> {

    private BestMatchStandard bestMatchStandard;

    public BestKeyMatchBlocker(BestMatchStandard bestMatchStandard) {
        this.bestMatchStandard = bestMatchStandard;
    }

    @Override
    public Processable<Aligner<RecordType, AlignerType>> createBlocking(DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, Processable<Aligner<AlignerType, Matchable>> schemaAligner) {
        ParallelProcessableCollection<Aligner<RecordType, AlignerType>> result = new ParallelProcessableCollection<Aligner<RecordType, AlignerType>>();

        for (Pair<String, String> positivePair : this.bestMatchStandard.getPositiveSamples()) {
            RecordType record1 = dataset1.getRecord(positivePair.getFirst());
            if (record1 != null) {
                RecordType record2 = dataset2.getRecord(positivePair.getSecond());
                if (record2 != null) {
                    result.add(new Aligner<RecordType, AlignerType>(record1, record2, 1.0, schemaAligner));
                }
            } else {
                record1 = dataset1.getRecord(positivePair.getSecond());
                if (record1 != null) {
                    RecordType record2 = dataset2.getRecord(positivePair.getFirst());
                    if (record2 != null) {
                        result.add(new Aligner<RecordType, AlignerType>(record1, record2, 1.0, schemaAligner));
                    }
                }
            }
        }

        for (Pair<String, String> negativePair : this.bestMatchStandard.getNegativeSamples()) {
            RecordType record1 = dataset1.getRecord(negativePair.getFirst());
            if (record1 != null) {
                RecordType record2 = dataset2.getRecord(negativePair.getSecond());
                if (record2 != null) {
                    result.add(new Aligner<RecordType, AlignerType>(record1, record2, 1.0, schemaAligner));
                }
            } else {
                record1 = dataset1.getRecord(negativePair.getSecond());
                if (record1 != null) {
                    RecordType record2 = dataset2.getRecord(negativePair.getFirst());
                    if (record2 != null) {
                        result.add(new Aligner<RecordType, AlignerType>(record1, record2, 1.0, schemaAligner));
                    }
                }
            }
        }
        log.info(String.format("Created %d blocked pairs from the goldstandard!", result.size()));
        return result;
    }
}
