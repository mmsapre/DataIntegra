package com.integration.em.blocker.generators;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.processing.*;
import com.integration.em.tables.Pair;

public abstract class BlockingKeyGenerator<RecordType extends Matchable, AlignerType extends Matchable, BlockedType extends Matchable>
        implements RecordKeyValueMapper<String, Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>, Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>,
        RecordMapper<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>, Pair<String, Pair<BlockedType,Processable<Aligner<AlignerType, Matchable>>>>> {


    @Override
    public void mapRecord(Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>> record, DataIterator<Pair<String, Pair<BlockedType,Processable<Aligner<AlignerType, Matchable>>>>> resultCollector) {
        mapRecordToKey(record, resultCollector);
    }

    @Override
    public void mapRecordToKey(Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>> record,
                               DataIterator<Pair<String, Pair<BlockedType,Processable<Aligner<AlignerType, Matchable>>>>> resultCollector) {

        ProcessableCollector<Pair<String, BlockedType>> collector = new ProcessableCollector<>();
        collector.setResult(new ProcessableCollection<>());
        collector.initialise();

        generateBlockingKeys(record.getFirst(), record.getSecond(), collector);

        collector.finalise();

        for(Pair<String, BlockedType> p : collector.getResult().get()) {
            resultCollector.next(new Pair<>(p.getFirst(), new Pair<>(p.getSecond(), record.getSecond())));
        }
    }


    public abstract void generateBlockingKeys(RecordType record, Processable<Aligner<AlignerType, Matchable>> correspondences, DataIterator<Pair<String, BlockedType>> resultCollector);
}
