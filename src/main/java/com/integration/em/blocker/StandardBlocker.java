package com.integration.em.blocker;

import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.LeftIdentityPair;
import com.integration.em.model.Matchable;
import com.integration.em.processing.*;
import com.integration.em.tables.Pair;
import com.integration.em.utils.Distribution;
import com.integration.em.utils.Q;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.integration.em.model.Record;

@Slf4j
public class StandardBlocker<RecordType extends Matchable, SchemaElementType extends Matchable, BlockedType extends Matchable, AlignerType extends Matchable>
        extends AbstractBlocker<RecordType, BlockedType, AlignerType>
        implements Blocker<RecordType, SchemaElementType, BlockedType, AlignerType>,
        SymmetricBlocker<RecordType, SchemaElementType, BlockedType, AlignerType> {

    private BlockingKeyGenerator<RecordType, AlignerType, BlockedType> blockedTypeBlockingKeyGenerator;
    private BlockingKeyGenerator<RecordType, AlignerType, BlockedType> secondBlockedTypeBlockingKeyGenerator;
    private double blockFilterRatio = 1.0;
    private int maxBlockPairSize = 0;
    private boolean deduplicatePairs = true;
    private boolean cacheBlocks = false;
    private Processable<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> grouped1;
    private Processable<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> grouped2;

    public StandardBlocker(BlockingKeyGenerator<RecordType, AlignerType, BlockedType> blockingFunction) {
        this.blockedTypeBlockingKeyGenerator = blockingFunction;
        this.secondBlockedTypeBlockingKeyGenerator = blockingFunction;
    }

    public StandardBlocker(BlockingKeyGenerator<RecordType, AlignerType, BlockedType> blockingFunction,
                           BlockingKeyGenerator<RecordType, AlignerType, BlockedType> secondBlockingFunction) {
        this.blockedTypeBlockingKeyGenerator = blockingFunction;
        this.secondBlockedTypeBlockingKeyGenerator = secondBlockingFunction == null ? blockingFunction : secondBlockingFunction;
    }
    public double getBlockFilterRatio() {
        return blockFilterRatio;
    }

    public void setBlockFilterRatio(double blockFilterRatio) {
        this.blockFilterRatio = blockFilterRatio;
    }

    public BlockingKeyGenerator<RecordType, AlignerType, BlockedType> getBlockedTypeBlockingKeyGenerator() {
        return blockedTypeBlockingKeyGenerator;
    }

    public void setBlockedTypeBlockingKeyGenerator(BlockingKeyGenerator<RecordType, AlignerType, BlockedType> blockedTypeBlockingKeyGenerator) {
        this.blockedTypeBlockingKeyGenerator = blockedTypeBlockingKeyGenerator;
    }

    public BlockingKeyGenerator<RecordType, AlignerType, BlockedType> getSecondBlockedTypeBlockingKeyGenerator() {
        return secondBlockedTypeBlockingKeyGenerator;
    }

    public void setSecondBlockedTypeBlockingKeyGenerator(BlockingKeyGenerator<RecordType, AlignerType, BlockedType> secondBlockedTypeBlockingKeyGenerator) {
        this.secondBlockedTypeBlockingKeyGenerator = secondBlockedTypeBlockingKeyGenerator;
    }

    public int getMaxBlockPairSize() {
        return maxBlockPairSize;
    }

    public void setMaxBlockPairSize(int maxBlockPairSize) {
        this.maxBlockPairSize = maxBlockPairSize;
    }

    public boolean isDeduplicatePairs() {
        return deduplicatePairs;
    }

    public void setDeduplicatePairs(boolean deduplicatePairs) {
        this.deduplicatePairs = deduplicatePairs;
    }

    public boolean isCacheBlocks() {
        return cacheBlocks;
    }

    public void setCacheBlocks(boolean cacheBlocks) {
        this.cacheBlocks = cacheBlocks;
    }

    public Processable<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> getGrouped1() {
        return grouped1;
    }

    public void setGrouped1(Processable<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> grouped1) {
        this.grouped1 = grouped1;
    }

    public Processable<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> getGrouped2() {
        return grouped2;
    }

    public void setGrouped2(Processable<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> grouped2) {
        this.grouped2 = grouped2;
    }

    public void resetCache(boolean dataset1, boolean dataset2) {
        if(dataset1) {
            grouped1 = null;
        }
        if(dataset2) {
            grouped2 = null;
        }
    }
    @Override
    public Processable<Aligner<BlockedType, AlignerType>> createBlocking(DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, Processable<Aligner<AlignerType, Matchable>> alignerProcessable) {

        if(!cacheBlocks || grouped1==null) {
            log.trace(String.format("Creating blocking key values for dataset1: %d records", dataset1.size()));
            Processable<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> ds1 = combineDataWithAligner(
                    dataset1, alignerProcessable,
                    (r, c) -> c.next(new Pair<>(r.getFirstRecordType().getDataSourceIdentifier(), r)));

            grouped1 = ds1
                    .aggregate(blockedTypeBlockingKeyGenerator,
                            new DistributionAggregator<String, Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>, Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>() {

                                private static final long serialVersionUID = 1L;

                                @Override
                                public Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>> getInnerKey(
                                        Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>> record) {

                                    return new LeftIdentityPair<>(record.getFirst(), record.getSecond());
                                }

                            });
        }

        if(!cacheBlocks || grouped2==null) {
            log.trace(String.format("Creating blocking key values for dataset2: %d records", dataset2.size()));
            Processable<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> ds2 = combineDataWithAligner(
                    dataset2, alignerProcessable,
                    (r, c) -> c.next(new Pair<>(r.getSecondRecordType().getDataSourceIdentifier(), r)));
            grouped2 = ds2
                    .aggregate(secondBlockedTypeBlockingKeyGenerator,
                            new DistributionAggregator<String, Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>, Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>() {

                                private static final long serialVersionUID = 1L;

                                @Override
                                public Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>> getInnerKey(
                                        Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>> record) {

                                    return new LeftIdentityPair<>(record.getFirst(), record.getSecond());
                                }

                            });
        }

        if (this.isMeasureBlockSizes()) {
            log.info(String.format("created %d blocking keys for first dataset", grouped1.size()));
            log.info(String.format("created %d blocking keys for second dataset", grouped2.size()));
        }

        log.trace(String.format("Joining blocking key values: %d x %d blocks", grouped1.size(), grouped2.size()));
        Processable<Pair<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>, Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>>> blockedData = grouped1
                .join(grouped2, new PairFirstJoinKeyGenerator<>());

        if (this.isMeasureBlockSizes()) {
            log.info(String.format("created %d blocks from blocking keys", blockedData.size()));
        }

        if (maxBlockPairSize > 0) {
            blockedData = blockedData.where((p) -> ((long) p.getFirst().getSecond().getNumElements()
                    * (long) p.getSecond().getSecond().getNumElements()) <= maxBlockPairSize);

            if (this.isMeasureBlockSizes()) {
                log.info(String.format("%d blocks after filtering by max block size (<= %d pairs)",
                        blockedData.size(), maxBlockPairSize));
            }
        }

        if (blockFilterRatio < 1.0) {
            log.info(String.format("%d blocks before filtering", blockedData.size()));

            Processable<Pair<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>, Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>>> toRemove = blockedData
                    .sort((p) -> p.getFirst().getSecond().getNumElements() * p.getSecond().getSecond().getNumElements(),
                            false)
                    .take((int) (blockedData.size() * (1 - blockFilterRatio)));

            if (this.isMeasureBlockSizes()) {
                for (Pair<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>, Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> p : toRemove
                        .get()) {
                    log.info(String.format("\tRemoving block '%s' (%d pairs)", p.getFirst().getFirst(),
                            p.getFirst().getSecond().getNumElements() * p.getSecond().getSecond().getNumElements()));
                }
            }

            blockedData = blockedData
                    .sort((p) -> p.getFirst().getSecond().getNumElements() * p.getSecond().getSecond().getNumElements(),
                            true)
                    .take((int) (blockedData.size() * blockFilterRatio));
            log.info(String.format("%d blocks after filtering", blockedData.size()));
        }

        if (this.isMeasureBlockSizes()) {

            // calculate block size distribution
            Processable<Pair<Integer, Distribution<Integer>>> aggregated = blockedData.aggregate(
                    (Pair<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>, Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> record,
                     DataIterator<Pair<Integer, Integer>> resultCollector) -> {
                        int blockSize = record.getFirst().getSecond().getNumElements()
                                * record.getSecond().getSecond().getNumElements();
                        resultCollector.next(new Pair<Integer, Integer>(0, blockSize));
                    }, new DistributionAggregator<Integer, Integer, Integer>() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Integer getInnerKey(Integer record) {
                            return record;
                        }
                    });

            Pair<Integer, Distribution<Integer>> aggregationResult = Q.firstOrDefault(aggregated.get());

            if (aggregationResult != null) {
                Distribution<Integer> dist = aggregationResult.getSecond();

                log.trace("Block size distribution:");
                log.trace(dist.format());

                // determine frequent blocking key values
                Processable<Pair<Integer, String>> blockValues = blockedData.aggregate(
                        (Pair<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>, Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> record,
                         DataIterator<Pair<Integer, String>> resultCollector) -> {
                            int blockSize = record.getFirst().getSecond().getNumElements()
                                    * record.getSecond().getSecond().getNumElements();
                            resultCollector.next(new Pair<Integer, String>(blockSize, record.getFirst().getFirst()));
                        }, new StringConcatAggregator<>(",")).sort((p) -> p.getFirst(), false);

                this.initializeBlockingResults();
                int result_id = 0;

                log.trace("Blocking key values:");
                log.trace(String.format("%s\t%s", "BlockingKeyValue", "Frequency"));
                for (Pair<Integer, String> value : blockValues.get()) {
                    Record model = new Record(Integer.toString(result_id));
                    model.setValue(AbstractBlocker.blockingKeyValue, value.getSecond().toString());
                    model.setValue(AbstractBlocker.frequency, value.getFirst().toString());
                    result_id += 1;

                    this.appendBlockingResult(model);

                    log.trace(String.format("%s\t\t\t%d", value.getSecond(), value.getFirst()));
                }
            } else {
                log.info("No blocks were created!");
            }

        }

        Processable<Aligner<BlockedType, AlignerType>> result = blockedData.map(
                new RecordMapper<Pair<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>, Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>>, Aligner<BlockedType, AlignerType>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void mapRecord(
                            Pair<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>, Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> record,
                            DataIterator<Aligner<BlockedType, AlignerType>> resultCollector) {

                        for (Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>> p1 : record
                                .getFirst().getSecond().getElements()) {

                            BlockedType record1 = p1.getFirst();


                            for (Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>> p2 : record
                                    .getSecond().getSecond().getElements()) {

                                BlockedType record2 = p2.getFirst();

                                if (alignerProcessable != null) {
                                    Processable<Aligner<AlignerType, Matchable>> causes = new ProcessableCollection<>(
                                            p1.getSecond()).append(p2.getSecond()).distinct();

                                    int[] pairIds = new int[] { p1.getFirst().getDataSourceIdentifier(),
                                            p2.getFirst().getDataSourceIdentifier() };
                                    Arrays.sort(pairIds);


                                    causes = causes.where((c) -> {

                                        int[] causeIds = new int[] { c.getFirstRecordType().getDataSourceIdentifier(),
                                                c.getSecondRecordType().getDataSourceIdentifier() };
                                        Arrays.sort(causeIds);

                                        return Arrays.equals(pairIds, causeIds);
                                    });

                                    resultCollector.next(new Aligner<BlockedType, AlignerType>(record1,
                                            record2, 1.0, causes));
                                } else {
                                    resultCollector.next(new Aligner<BlockedType, AlignerType>(record1,
                                            record2, 1.0, null));
                                }

                            }

                        }
                    }
                });

        if (deduplicatePairs) {

            result = result.distinct();
        }

        calculatePerformance(dataset1, dataset2, result);


        return result;
    }


    @Override
    public Processable<Aligner<BlockedType, AlignerType>> createBlocking(DataSet<RecordType, SchemaElementType> dataset, Processable<Aligner<AlignerType, Matchable>> alignerProcessable) {

        Processable<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> ds = combineDataWithAligner(
                dataset, alignerProcessable, (r, c) -> {
                    c.next(new Pair<>(r.getFirstRecordType().getDataSourceIdentifier(), r));
                    c.next(new Pair<>(r.getSecondRecordType().getDataSourceIdentifier(), r));
                });

        Processable<Pair<String, Distribution<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>>> grouped = ds
                .aggregate(blockedTypeBlockingKeyGenerator,
                        new DistributionAggregator<String, Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>, Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>() {

                            @Override
                            public Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>> getInnerKey(
                                    Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>> record) {

                                return new LeftIdentityPair<>(record.getFirst(), record.getSecond());
                            }
                        });

        Processable<Aligner<BlockedType, AlignerType>> blocked = grouped.map((g, collector) -> {
            List<Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>> list = new ArrayList<>(
                    g.getSecond().getElements());
            list.sort((o1, o2) -> Integer.compare(o1.getFirst().getDataSourceIdentifier(),
                    o2.getFirst().getDataSourceIdentifier()));
            for (int i = 0; i < list.size(); i++) {
                Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>> p1 = list.get(i);
                for (int j = i + 1; j < list.size(); j++) {
                    Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>> p2 = list.get(j);

                    Processable<Aligner<AlignerType, Matchable>> causes = new ProcessableCollection<>(
                            p1.getSecond()).append(p2.getSecond());

                    int[] pairIds = new int[] { p1.getFirst().getDataSourceIdentifier(),
                            p2.getFirst().getDataSourceIdentifier() };
                    Arrays.sort(pairIds);


                    causes = causes.where((c) -> {
                        int[] causeIds = new int[] { c.getFirstRecordType().getDataSourceIdentifier(),
                                c.getSecondRecordType().getDataSourceIdentifier() };
                        Arrays.sort(causeIds);

                        return Arrays.equals(pairIds, causeIds);
                    }).distinct();

                    collector.next(new Aligner<>(p1.getFirst(), p2.getFirst(), 1.0, causes));
                }
            }
        });

        blocked = blocked.distinct();

        calculatePerformance(dataset, dataset, blocked);
        return blocked;
    }
}
