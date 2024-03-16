package com.integration.em.blocker;

import com.integration.em.aggregator.SetAggregator;
import com.integration.em.aggregator.SumDoubleAggregator;
import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.LeftIdentityPair;
import com.integration.em.model.Matchable;
import com.integration.em.processing.*;
import com.integration.em.similarity.VectorSpaceSimilarity;
import com.integration.em.tables.Pair;
import com.integration.em.utils.Function;
import com.integration.em.utils.Q;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class BlockingKeyIndexer<RecordType extends Matchable, SchemaElementType extends Matchable, BlockedType extends Matchable, AlignerType extends Matchable>
        extends AbstractBlocker<RecordType, BlockedType, AlignerType>
        implements Blocker<RecordType, SchemaElementType, BlockedType, AlignerType> {

    public enum VectorCreationMethod {
        BinaryTermOccurrences, TermFrequencies, TFIDF
    }

    public enum DocumentFrequencyCounter {
        Dataset1, Dataset2, Both, Preset
    }

    private VectorCreationMethod vectorCreationMethod;
    private double similarityThreshold;
    private DocumentFrequencyCounter documentFrequencyCounter = DocumentFrequencyCounter.Both;
    private Processable<Pair<String, Double>> inverseDocumentFrequencies;

    private BlockingKeyGenerator<RecordType, AlignerType, BlockedType> blockedTypeBlockingKeyGenerator;
    private BlockingKeyGenerator<RecordType, AlignerType, BlockedType> secondBlockedTypeBlockingKeyGenerator;
    private VectorSpaceSimilarity vectorSpaceSimilarity;


    public BlockingKeyIndexer(BlockingKeyGenerator<RecordType, AlignerType, BlockedType> alignerTypeBlockedTypeBlockingKeyGenerator,
                              BlockingKeyGenerator<RecordType, AlignerType, BlockedType> secondBlockingFunction,
                              VectorSpaceSimilarity similarityFunction, VectorCreationMethod vectorCreationMethod,
                              double similarityThreshold) {
        this.blockedTypeBlockingKeyGenerator = alignerTypeBlockedTypeBlockingKeyGenerator;
        this.secondBlockedTypeBlockingKeyGenerator = secondBlockingFunction == null ? alignerTypeBlockedTypeBlockingKeyGenerator : secondBlockingFunction;
        this.vectorSpaceSimilarity = similarityFunction;
        this.vectorCreationMethod = vectorCreationMethod;
        this.similarityThreshold = similarityThreshold;
    }

    @Override
    public Processable<Aligner<BlockedType, AlignerType>> createBlocking(DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, Processable<Aligner<AlignerType, Matchable>> schemaAligner) {
        return null;
    }

//
//    public Processable<Pair<String, Double>> calculateInverseDocumentFrequencies(
//            DataSet<RecordType, SchemaElementType> dataset,
//            BlockingKeyGenerator<RecordType, AlignerType, BlockedType> blockedTypeBlockingKeyGenerator) {
//
//        DocumentFrequencyCounter documentFrequencyCounter = DocumentFrequencyCounter.Dataset1;
//
//        Processable<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> ds = combineDataWithAligner(
//                dataset, null,
//                (r, c) -> c.next(new Pair<>(r.getFirstRecordType().getDataSourceIdentifier(), r)));
//
//        log.info("Creating blocking key value vectors");
//        Processable<Pair<BlockedType, BlockingVector>> vectors1 = createBlockingVectors(ds, blockedTypeBlockingKeyGenerator);
//
//        log.info("Creating inverted index");
//        Processable<Block> blocks1 = createInvertedIndex(vectors1);
//
//        log.info("Calculating TFIDF vectors");
//        // update blocking key value vectors to TF-IDF weights
//
//        Processable<Pair<String, Double>> documentFrequencies = createDocumentFrequencies(blocks1, new ParallelProcessableCollection<Block>(), documentFrequencyCounter);
//        int documentCount = getDocumentCount(vectors1, new ParallelProcessableCollection<Pair<BlockedType, BlockingVector>>(), documentFrequencyCounter);
//        Processable<Pair<String, Double>> inverseDocumentFrequencies = createIDF(documentFrequencies, documentCount);
//
//        return inverseDocumentFrequencies;
//    }
    public VectorSpaceSimilarity getVectorSpaceSimilarity() {
        return vectorSpaceSimilarity;
    }

    public DocumentFrequencyCounter getDocumentFrequencyCounter() {
        return documentFrequencyCounter;
    }

    public Processable<Pair<String, Double>> getInverseDocumentFrequencies() {
        return inverseDocumentFrequencies;
    }

    public Processable<Pair<String, Double>> calculateInverseDocumentFrequencies(
            DataSet<RecordType, SchemaElementType> dataset,
            BlockingKeyGenerator<RecordType, AlignerType, BlockedType> blockingFunction) {

        DocumentFrequencyCounter documentFrequencyCounter = DocumentFrequencyCounter.Dataset1;

        Processable<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> ds = combineDataWithAligner(
                dataset, null,
                (r, c) -> c.next(new Pair<>(r.getFirstRecordType().getDataSourceIdentifier(), r)));

        log.info("Creating blocking key value vectors");
        Processable<Pair<BlockedType, BlockingVector>> vectors1 = createBlockingVectors(ds, blockingFunction);

        log.info("Creating inverted index");
        Processable<Block> blocks1 = createInvertedIndex(vectors1);

        log.info("Calculating TFIDF vectors");
        // update blocking key value vectors to TF-IDF weights

        Processable<Pair<String, Double>> documentFrequencies = createDocumentFrequencies(blocks1, new ParallelProcessableCollection<Block>(), documentFrequencyCounter);
        int documentCount = getDocumentCount(vectors1, new ParallelProcessableCollection<Pair<BlockedType, BlockingVector>>(), documentFrequencyCounter);
        Processable<Pair<String, Double>> inverseDocumentFrequencies = createIDF(documentFrequencies, documentCount);

        return inverseDocumentFrequencies;
    }

    protected Processable<Pair<BlockedType, BlockingVector>> createBlockingVectors(
            Processable<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> ds,
            BlockingKeyGenerator<RecordType, AlignerType, BlockedType> blockingFunction) {

        return ds.aggregate(
                new RecordKeyValueMapper<BlockedType, Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>, Pair<String, Processable<Aligner<AlignerType, Matchable>>>>() {

                    @Override
                    public void mapRecordToKey(
                            Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>> record,
                            DataIterator<Pair<BlockedType, Pair<String, Processable<Aligner<AlignerType, Matchable>>>>> resultCollector) {

                        Processable<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> alg = new ProcessableCollection<>();
                        alg.add(record);
                        Processable<Pair<String, Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>>> blockingKeyValues = alg
                                .map(blockingFunction);


                        for (Pair<String, Pair<BlockedType, Processable<Aligner<AlignerType, Matchable>>>> p : blockingKeyValues
                                .get()) {
                            BlockedType blocked = p.getSecond().getFirst();
                            String blockingKeyValue = p.getFirst();
                            Processable<Aligner<AlignerType, Matchable>> correspondences = p.getSecond()
                                    .getSecond();
                            resultCollector.next(new Pair<>(blocked, new Pair<>(blockingKeyValue, correspondences)));
                        }
                    }
                },

                new DataAggregator<BlockedType, Pair<String, Processable<Aligner<AlignerType, Matchable>>>, BlockingVector>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Pair<BlockingVector, Object> initialise(BlockedType keyValue) {
                        return stateless(new BlockingVector());
                    }

                    @Override
                    public Pair<BlockingVector, Object> aggregate(BlockingVector previousResult,
                                                                  Pair<String, Processable<Aligner<AlignerType, Matchable>>> record,
                                                                  Object state) {


                        Double existing = previousResult.get(record.getFirst());

                        if (existing == null) {

                            existing = 0.0;

                        }

                        Double frequency = existing + 1;


                        existing = frequency;

                        previousResult.put(record.getFirst(), existing);
                        previousResult.addCorrespondences(record.getSecond());
                        return stateless(previousResult);
                    }


                    @Override
                    public Pair<BlockingVector, Object> merge(
                            Pair<BlockingVector, Object> intermediateResult1,
                            Pair<BlockingVector, Object> intermediateResult2) {

                        BlockingVector first = intermediateResult1.getFirst();
                        BlockingVector second = intermediateResult2.getFirst();

                        Set<String> keys = Q.union(first.keySet(), second.keySet());

                        BlockingVector result = new BlockingVector();
                        result.addCorrespondences(first.getAlignerProcessable());
                        result.addCorrespondences(second.getAlignerProcessable());

                        for (String k : keys) {

                            Double v1 = first.get(k);
                            Double v2 = second.get(k);

                            if (v1 == null) {
                                v1 = v2;
                            } else if (v2 != null) {

                                v1 = v1 + v2;


                            }

                            result.put(k, v1);
                        }

                        return stateless(result);
                    }


                    @Override
                    public BlockingVector createFinalValue(
                            BlockedType keyValue,
                            BlockingVector result,
                            Object state) {

                        BlockingVector vector = new BlockingVector();
                        vector.addCorrespondences(result.getAlignerProcessable());

                        for (String s : result.keySet()) {

                            Double d = result.get(s);

                            if (vectorCreationMethod == VectorCreationMethod.BinaryTermOccurrences) {

                                d = Math.min(1.0, d);
                            } else {

                                d = d / result.size();
                            }

                            vector.put(s, d);
                        }

                        return vector;
                    }
                });
    }

    protected Processable<Block> createInvertedIndex(Processable<Pair<BlockedType, BlockingVector>> vectors) {

        return vectors.aggregate(
                (Pair<BlockedType, BlockingVector> record,
                 DataIterator<Pair<String, BlockedType>> resultCollector) -> {

                    for (String s : record.getSecond().keySet()) {
                        resultCollector.next(new Pair<>(s, record.getFirst()));
                    }

                }, new SetAggregator<>()).map((Pair<String, Set<BlockedType>> record,
                                               DataIterator<Block> resultCollector) -> {

            resultCollector.next(new Block(record.getFirst(), record.getSecond()));
            ;

        });

    }

    protected Processable<Pair<String, Double>> createDocumentFrequencies(Processable<Block> blocks1,
                                                                          Processable<Block> blocks2, DocumentFrequencyCounter documentFrequencyCounter) {

        Processable<Pair<String, Double>> df1 = blocks1
                .map((Block record,
                      DataIterator<Pair<String, Double>> resultCollector) -> {
                    resultCollector.next(new Pair<>(record.getFirst(), (double) record.getSecond().size()));
                });

        Processable<Pair<String, Double>> df2 = blocks2
                .map((Block record,
                      DataIterator<Pair<String, Double>> resultCollector) -> {
                    resultCollector.next(new Pair<>(record.getFirst(), (double) record.getSecond().size()));
                });

        Processable<Pair<String, Double>> df = null;

        switch(documentFrequencyCounter) {
            case Dataset1:
                df = df1;
                break;
            case Dataset2:
                df = df2;
                break;
            default:
                df = df1.append(df2);
        }

        return df
                .aggregate((Pair<String, Double> record, DataIterator<Pair<String, Double>> resultCollector) -> {
                    resultCollector.next(record);
                }, new SumDoubleAggregator<>());
    }

    protected int getDocumentCount(Processable<Pair<BlockedType, BlockingVector>> vectors1, Processable<Pair<BlockedType, BlockingVector>> vectors2, DocumentFrequencyCounter documentFrequencyCounter) {
        switch(documentFrequencyCounter) {
            case Dataset1:
                return vectors1.size();
            case Dataset2:
                return vectors2.size();
            default:
                return vectors1.size() + vectors2.size();
        }
    }

    protected Processable<Pair<String, Double>> createIDF(Processable<Pair<String, Double>> documentFrequencies, int documentCount) {
        return documentFrequencies.map((f)->new Pair<String, Double>(f.getFirst(), Math.log(documentCount / f.getSecond())));
    }

    protected Processable<Pair<BlockedType, BlockingVector>> createTFIDFVectors(
            Processable<Pair<BlockedType, BlockingVector>> vectors,
            Processable<Pair<String, Double>> inverseDocumentFrequencies) {

        Map<String, Double> idfMap = Q.map(inverseDocumentFrequencies.get(), (p) -> p.getFirst(), (p) -> p.getSecond());

        return vectors.map((
                Pair<BlockedType, BlockingVector> record,
                DataIterator<Pair<BlockedType, BlockingVector>> resultCollector) -> {
            BlockingVector tfVector = record.getSecond();
            BlockingVector tfIdfVector = new BlockingVector();

            for (String s : tfVector.keySet()) {
                Double tfScore = tfVector.get(s);

                Double idf = idfMap.get(s);
                if(idf==null) {
                    idf = 0.0;
                }
                double tfIdfScore = tfScore * idf;

                tfIdfVector.put(s, tfIdfScore);
            }

            resultCollector.next(new Pair<>(record.getFirst(), tfIdfVector));
        });

    }


    protected Processable<Aligner<AlignerType, Matchable>> createCausalAligners(
            BlockedType record1, BlockedType record2, BlockingVector vector1, BlockingVector vector2) {

        Processable<Aligner<AlignerType, Matchable>> causes = new ProcessableCollection<>(
                vector1.getAlignerProcessable().get()).append(vector2.getAlignerProcessable()).distinct();

        int[] pairIds = new int[] { record1.getDataSourceIdentifier(), record2.getDataSourceIdentifier() };
        Arrays.sort(pairIds);

        return causes.where((c) -> {

            int[] causeIds = new int[] { c.getFirstRecordType().getDataSourceIdentifier(),
                    c.getSecondRecordType().getDataSourceIdentifier() };
            Arrays.sort(causeIds);

            return Arrays.equals(pairIds, causeIds);
        });
    }

    protected class BlockingVector extends HashMap<String, Double> {


        private Processable<Aligner<AlignerType, Matchable>> alignerProcessable = new ProcessableCollection<>();

        public Processable<Aligner<AlignerType, Matchable>> getAlignerProcessable() {
            return alignerProcessable;
        }

        public void setAlignerProcessable(Processable<Aligner<AlignerType, Matchable>> alignerProcessable) {
            this.alignerProcessable = alignerProcessable;
        }

        public void addCorrespondences(Processable<Aligner<AlignerType, Matchable>> alignerProcessable) {
            this.alignerProcessable = this.alignerProcessable.append(alignerProcessable);
        }
    }

    protected class Block extends LeftIdentityPair<String, Set<BlockedType>> {


        public Block(String first, Set<BlockedType> second) {
            super(first, second);
        }
    }

    protected class BlockJoinKeyGenerator implements Function<String, Block> {

        @Override
        public String execute(Block input) {
            return input.getFirst();
        }

    }
}
