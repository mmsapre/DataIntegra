package com.integration.em.blocker;

import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.*;
import com.integration.em.processing.*;
import com.integration.em.tables.Pair;
import com.integration.em.utils.Distribution;
import com.integration.em.utils.Function;
import com.integration.em.utils.Q;
import lombok.extern.slf4j.Slf4j;
import com.integration.em.model.Record;

@Slf4j
public class ValueBasedBlocker<RecordType extends Matchable, SchemaElementType extends Matchable, BlockedType extends Matchable>
        extends AbstractBlocker<RecordType, BlockedType, MatchableValue>
        implements Blocker<RecordType, SchemaElementType, BlockedType, MatchableValue>,
        SymmetricBlocker<RecordType, SchemaElementType, BlockedType, MatchableValue> {


    private BlockingKeyGenerator<RecordType, MatchableValue, BlockedType> blockedTypeBlockingKeyGenerator;
    private BlockingKeyGenerator<RecordType, MatchableValue, BlockedType> secondBlockedTypeBlockingKeyGenerator;
    private boolean considerDuplicateValues = false;


    public ValueBasedBlocker(BlockingKeyGenerator<RecordType, MatchableValue, BlockedType> blockedTypeBlockingKeyGenerator) {
        this.blockedTypeBlockingKeyGenerator = blockedTypeBlockingKeyGenerator;
        this.secondBlockedTypeBlockingKeyGenerator = blockedTypeBlockingKeyGenerator;
    }

    public ValueBasedBlocker(BlockingKeyGenerator<RecordType, MatchableValue, BlockedType> blockingFunction, BlockingKeyGenerator<RecordType, MatchableValue, BlockedType> secondBlockingFunction) {
        this.blockedTypeBlockingKeyGenerator = blockingFunction;
        this.secondBlockedTypeBlockingKeyGenerator = secondBlockingFunction == null ? blockingFunction : secondBlockingFunction;
    }

    public boolean isConsiderDuplicateValues() {
        return considerDuplicateValues;
    }

    public void setConsiderDuplicateValues(boolean considerDuplicateValues) {
        this.considerDuplicateValues = considerDuplicateValues;
    }

    @Override
    public Processable<Aligner<BlockedType, MatchableValue>> createBlocking(DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, Processable<Aligner<MatchableValue, Matchable>> schemaCorrespondences) {

        Processable<Pair<RecordType, Processable<Aligner<MatchableValue, Matchable>>>> ds1 = combineDataWithAligner(dataset1, schemaCorrespondences, (r, c) -> c.next(new Pair<>(r.getFirstRecordType().getDataSourceIdentifier(), r)));
        Processable<Pair<RecordType, Processable<Aligner<MatchableValue, Matchable>>>> ds2 = combineDataWithAligner(dataset2, schemaCorrespondences, (r, c) -> c.next(new Pair<>(r.getSecondRecordType().getDataSourceIdentifier(), r)));

        Processable<Block> grouped1 =
                ds1.aggregate(blockedTypeBlockingKeyGenerator, new DistributionAggregator<String, Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>>, Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>>>() {


                            @Override
                            public Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>> getInnerKey(
                                    Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>> record) {
                                return new LeftIdentityPair<>(record.getFirst(), record.getSecond());
                            }

                        })
                        .map((Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>>>> record, DataIterator<Block> resultCollector)
                                        -> {
                                    resultCollector.next(new Block(record.getFirst(), record.getSecond()));
                                }
                        );


        Processable<Block> grouped2 =
                ds2.aggregate(secondBlockedTypeBlockingKeyGenerator, new DistributionAggregator<String, Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>>, Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>>>() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>> getInnerKey(
                                    Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>> record) {
                                return new LeftIdentityPair<>(record.getFirst(), record.getSecond());
                            }

                        })
                        .map((Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>>>> record, DataIterator<Block> resultCollector)
                                        -> {
                                    resultCollector.next(new Block(record.getFirst(), record.getSecond()));
                                }
                        );

        Processable<Pair<Block, Block>> blockedData = grouped1.join(grouped2, new BlockJoinKeyGenerator());

        if (this.isMeasureBlockSizes()) {
            // calculate block size distribution
            Processable<Pair<Integer, Distribution<Integer>>> aggregated = blockedData.aggregate(
                    (Pair<Block, Block> record,
                     DataIterator<Pair<Integer, Integer>> resultCollector)
                            -> {
                        int blockSize = record.getFirst().getSecond().getNumElements() * record.getSecond().getSecond().getNumElements();
                        resultCollector.next(new Pair<Integer, Integer>(0, blockSize));
                    }
                    , new DistributionAggregator<Integer, Integer, Integer>() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Integer getInnerKey(Integer record) {
                            return record;
                        }
                    });
            Distribution<Integer> dist = Q.firstOrDefault(aggregated.get()).getSecond();

            log.info("Block size distribution:");
            log.info(dist.format());

            Processable<Pair<Integer, String>> blockValues = blockedData.aggregate(
                            (Pair<Block, Block> record, DataIterator<Pair<Integer, String>> resultCollector)
                                    -> {
                                int blockSize = record.getFirst().getSecond().getNumElements() * record.getSecond().getSecond().getNumElements();
                                resultCollector.next(new Pair<Integer, String>(blockSize, record.getFirst().getFirst()));
                            }
                            , new StringConcatAggregator<>(","))
                    .sort((p) -> p.getFirst(), false);

            this.initializeBlockingResults();
            int result_id = 0;
            log.info("Blocking key values:");
            for(Pair<Integer, String> value : blockValues.get()) {
                Record model = new Record(Integer.toString(result_id));
                model.setValue(AbstractBlocker.blockingKeyValue, value.getFirst().toString());
                model.setValue(AbstractBlocker.frequency, value.getFirst().toString());
                result_id += 1;
                this.appendBlockingResult(model);

                log.info(String.format("\t%d\t%s", value.getFirst(), value.getSecond()));
            }
        }

        Processable<Aligner<BlockedType, MatchableValue>> result
                = blockedData.map(new RecordMapper<Pair<Block,Block>, Aligner<BlockedType, MatchableValue>>() {

            @Override
            public void mapRecord(
                    Pair<Block, Block> record,
                    DataIterator<Aligner<BlockedType, MatchableValue>> resultCollector) {

                Distribution<Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>>> dist1 = record.getFirst().getSecond();

                for(Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>> p1 : dist1.getElements()){

                    BlockedType record1 = p1.getFirst();

                    int record1Frequency = dist1.getFrequency(p1);

                    Distribution<Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>>> dist2 = record.getSecond().getSecond();

                    for(Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>> p2 : dist2.getElements()){

                        BlockedType record2 = p2.getFirst();


                        int record2Frequency = dist2.getFrequency(p2);

                        double matchCount = 0.0;
                        if(considerDuplicateValues) {
                            matchCount = Math.max(record1Frequency, record2Frequency);
                        } else {
                            matchCount = Math.min(record1Frequency, record2Frequency);
                        }


                        Processable<Aligner<MatchableValue, Matchable>> causes = new ProcessableCollection<>();

                        Aligner<MatchableValue, Matchable> c1 = p1.getSecond().firstOrNull();
                        Aligner<MatchableValue, Matchable> c2 = p2.getSecond().firstOrNull();

                        causes.add(new Aligner<>(c1.getFirstRecordType(), c2.getFirstRecordType(), matchCount));

                        resultCollector.next(new Aligner<>(record1, record2, matchCount, causes));

                    }

                }
            }
        });

        return result;
    }

    @Override
    public Processable<Aligner<BlockedType, MatchableValue>> createBlocking
            (DataSet < RecordType, SchemaElementType > dataset, Processable < Aligner < MatchableValue, Matchable >> alignerProcessable)
    {
        return null;
    }

    class Block extends LeftIdentityPair<String, Distribution<Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>>>> {
        private static final long serialVersionUID = 1L;

        public Block(String first, Distribution<Pair<BlockedType, Processable<Aligner<MatchableValue, Matchable>>>> second) {
            super(first, second);
        }
    }

    class BlockJoinKeyGenerator implements Function<String, Block> {


        @Override
        public String execute(Block input) {
            return input.getFirst();
        }

    }
}
