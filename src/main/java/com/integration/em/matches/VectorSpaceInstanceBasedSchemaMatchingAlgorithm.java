package com.integration.em.matches;

import com.integration.em.blocker.BlockingKeyIndexer;
import com.integration.em.blocker.InstanceBasedBlockingKeyIndexer;
import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.model.MatchableValue;
import com.integration.em.processing.Processable;
import com.integration.em.similarity.VectorSpaceSimilarity;

public class VectorSpaceInstanceBasedSchemaMatchingAlgorithm<RecordType extends Matchable, SchemaElementType extends Matchable> implements MatchingAlgorithm<SchemaElementType, MatchableValue> {

    private DataSet<RecordType, SchemaElementType> dataset1;
    private DataSet<RecordType, SchemaElementType> dataset2;
    private BlockingKeyGenerator<RecordType, MatchableValue, SchemaElementType> blockingfunction1;
    private BlockingKeyGenerator<RecordType, MatchableValue, SchemaElementType> blockingfunction2;
    private BlockingKeyIndexer.VectorCreationMethod vectorCreation;
    private VectorSpaceSimilarity similarity;
    private double similarityThreshold;
    private Processable<Aligner<SchemaElementType, MatchableValue>> result;

    public VectorSpaceInstanceBasedSchemaMatchingAlgorithm(DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, BlockingKeyGenerator<RecordType, MatchableValue, SchemaElementType> blockingfunction1, BlockingKeyGenerator<RecordType, MatchableValue, SchemaElementType> blockingfunction2, BlockingKeyIndexer.VectorCreationMethod vectorCreation, VectorSpaceSimilarity similarity, double similarityThreshold) {
        this.dataset1 = dataset1;
        this.dataset2 = dataset2;
        this.blockingfunction1 = blockingfunction1;
        this.blockingfunction2 = blockingfunction2;
        this.vectorCreation = vectorCreation;
        this.similarity = similarity;
        this.similarityThreshold = similarityThreshold;
    }

    @Override
    public void run() {

        InstanceBasedBlockingKeyIndexer<RecordType, SchemaElementType, SchemaElementType> blocker = new InstanceBasedBlockingKeyIndexer<>(blockingfunction1, blockingfunction2, similarity, vectorCreation, similarityThreshold);

        result = blocker.createBlocking(dataset1, dataset2, null);

    }

    @Override
    public Processable<Aligner<SchemaElementType, MatchableValue>> getResult() {
        return result;
    }
}
