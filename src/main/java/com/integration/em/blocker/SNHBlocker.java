package com.integration.em.blocker;

import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.processing.Processable;
import com.integration.em.processing.ProcessableCollection;
import com.integration.em.tables.Pair;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import com.integration.em.model.Record;

@Slf4j
public class SNHBlocker<RecordType extends Matchable, SchemaElementType extends Matchable, AlignerType extends Matchable>
        extends AbstractBlocker<RecordType, SchemaElementType, AlignerType>
        implements Blocker<RecordType, SchemaElementType, RecordType, AlignerType>,
        SymmetricBlocker<RecordType, SchemaElementType, RecordType, AlignerType> {

    private BlockingKeyGenerator<RecordType, AlignerType, RecordType> blockingKeyGenerator;
    private int windowSize;

    public SNHBlocker(BlockingKeyGenerator<RecordType, AlignerType, RecordType> blockingKeyGenerator, int windowSize) {
        this.blockingKeyGenerator = blockingKeyGenerator;
        this.windowSize = windowSize;
    }

    public BlockingKeyGenerator<RecordType, AlignerType, RecordType> getBlockingKeyGenerator() {
        return blockingKeyGenerator;
    }

    @Override
    public Processable<Aligner<RecordType, AlignerType>> createBlocking(DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, Processable<Aligner<AlignerType, Matchable>> schemaAligner) {

        Processable<Aligner<RecordType, AlignerType>> result = new ProcessableCollection<>();

        Processable<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> ds1 = combineDataWithAligner(
                dataset1, schemaAligner,
                (r, c) -> c.next(new Pair<>(r.getFirstRecordType().getDataSourceIdentifier(), r)));
        Processable<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> ds2 = combineDataWithAligner(
                dataset2, schemaAligner,
                (r, c) -> c.next(new Pair<>(r.getFirstRecordType().getDataSourceIdentifier(), r)));

        Processable<Pair<String, Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>>> blocked1 = ds1
                .map(blockingKeyGenerator);
        ArrayList<Pair<String, Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>>> keyIdentifierList = new ArrayList<Pair<String, Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>>>(
                blocked1.get());

        Processable<Pair<String, Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>>> blocked2 = ds2
                .map(blockingKeyGenerator);
        keyIdentifierList.addAll(blocked2.get());

        Comparator<Pair<String, Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>>> pairComparator = new Comparator<Pair<String, Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>>>() {

            @Override
            public int compare(
                    Pair<String, Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> o1,
                    Pair<String, Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> o2) {
                return o1.getFirst().compareTo(o2.getFirst());
            }

        };

        Collections.sort(keyIdentifierList, pairComparator);

        HashMap<String, Integer> keyCounter = null;
        if(isMeasureBlockSizes()){
            keyCounter =new HashMap<String, Integer>();
        }

        for (int i = 0; i < keyIdentifierList.size() - 1; i++) {
            Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>> p1 = keyIdentifierList.get(i)
                    .getSecond();

            // make sure r1 belongs to dataset1
            if (dataset1.getRecord(p1.getFirst().getIdentifier()) != null) {

                int counter = 1;
                int j = i;
                while ((counter < windowSize) && (j < (keyIdentifierList.size() - 1))) {
                    Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>> p2 = keyIdentifierList
                            .get(++j).getSecond();
                    if (!p2.getFirst().getProvenance().equals(p1.getFirst().getProvenance())) {
                        result.add(new Aligner<RecordType, AlignerType>(p1.getFirst(), p2.getFirst(), 1.0,
                                createCausalCorrespondences(p1, p2)));
                        counter++;
                    }
                }

            }
            if(isMeasureBlockSizes()){
                String key = keyIdentifierList.get(i).getFirst();
                int count = 0;
                if(keyCounter.containsKey(key)){
                    count = keyCounter.get(key);
                }
                count++;
                keyCounter.put(key, count);
            }
        }

        if(isMeasureBlockSizes()){
            for(String key : keyCounter.keySet()){
                if(keyCounter.containsKey(key)){
                    Record model = new Record(key);
                    model.setValue(AbstractBlocker.blockingKeyValue, key);
                    model.setValue(AbstractBlocker.frequency, Integer.toString(keyCounter.get(key)));
                    this.appendBlockingResult(model);
                }
            }
        }

        calculatePerformance(dataset1, dataset2, result);

        return result;
    }

    @Override
    public Processable<Aligner<RecordType, AlignerType>> createBlocking(DataSet<RecordType, SchemaElementType> dataset, Processable<Aligner<AlignerType, Matchable>> alignerProcessable) {
        return null;
    }
}
