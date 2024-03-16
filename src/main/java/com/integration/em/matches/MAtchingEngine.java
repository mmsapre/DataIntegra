package com.integration.em.matches;

import com.integration.em.aggregator.AlignedAggregator;
import com.integration.em.aggregator.TopKVotesAggregator;
import com.integration.em.blocker.Blocker;
import com.integration.em.blocker.BlockingKeyIndexer;
import com.integration.em.blocker.NoSchemaBlocker;
import com.integration.em.blocker.SymmetricBlocker;
import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.model.MatchableValue;
import com.integration.em.processing.Processable;
import com.integration.em.rules.LinearCombinationMatchingRule;
import com.integration.em.rules.MatchingRule;
import com.integration.em.rules.compare.IComparator;
import com.integration.em.similarity.VectorSpaceSimilarity;
import com.integration.em.similarity.VotingMatchingRule;

public class MAtchingEngine<RecordType extends Matchable, SchemaElementType extends Matchable> {

    public MAtchingEngine() {
    }

    public Processable<Aligner<RecordType, SchemaElementType>> runIdentityResolution(
            DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
            Processable<? extends Aligner<SchemaElementType, ? extends Matchable>> processable,
            MatchingRule<RecordType, SchemaElementType> rule,
            Blocker<RecordType, SchemaElementType, RecordType, SchemaElementType> blocker) {

        RuleBasedMatchingAlgorithm<RecordType, SchemaElementType, SchemaElementType> algorithm = new RuleBasedMatchingAlgorithm<>(
                dataset1, dataset2, Aligner.toMatchable(processable), rule, blocker);
        algorithm.setTaskName("Identity Resolution");

        algorithm.run();

        return algorithm.getResult();

    }

    public Processable<Aligner<RecordType, SchemaElementType>> runBlocking(
            DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
            Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable,
            Blocker<RecordType, SchemaElementType, RecordType, SchemaElementType> blocker) {

        RuleBasedMatchingAlgorithm<RecordType, SchemaElementType, SchemaElementType> algorithm = new RuleBasedMatchingAlgorithm<>(
                dataset1, dataset2, Aligner.toMatchable(alignerProcessable), null, blocker);
        algorithm.setTaskName("Blocking");

        algorithm.createBlocking();

        return algorithm.getResult();

    }

    public Processable<Aligner<SchemaElementType, RecordType>> runSchemaMatching(
            DataSet<SchemaElementType, SchemaElementType> schema1,
            DataSet<SchemaElementType, SchemaElementType> schema2,
            Processable<? extends Aligner<RecordType, ? extends Matchable>> instanceAligner,
            MatchingRule<SchemaElementType, RecordType> rule,
            Blocker<SchemaElementType, SchemaElementType, SchemaElementType, RecordType> blocker) {

        RuleBasedMatchingAlgorithm<SchemaElementType, SchemaElementType, RecordType> algorithm = new RuleBasedMatchingAlgorithm<>(
                schema1, schema2, Aligner.toMatchable(instanceAligner), rule, blocker);
        algorithm.setTaskName("Schema Matching");

        algorithm.run();

        return algorithm.getResult();

    }

    public Processable<Aligner<SchemaElementType, SchemaElementType>> runLabelBasedSchemaMatching(
            DataSet<SchemaElementType, SchemaElementType> schema1,
            DataSet<SchemaElementType, SchemaElementType> schema2,
            IComparator<SchemaElementType, SchemaElementType> labelComparator, double similarityThreshold)
            throws Exception {

        Blocker<SchemaElementType, SchemaElementType, SchemaElementType, SchemaElementType> blocker = new NoSchemaBlocker<>();

        LinearCombinationMatchingRule<SchemaElementType, SchemaElementType> rule = new LinearCombinationMatchingRule<>(
                similarityThreshold);
        rule.addComparator(labelComparator, 1.0);

        RuleBasedMatchingAlgorithm<SchemaElementType, SchemaElementType, SchemaElementType> algorithm = new RuleBasedMatchingAlgorithm<>(
                schema1, schema2, null, rule, blocker);
        algorithm.setTaskName("Schema Matching");

        algorithm.run();

        return algorithm.getResult();

    }


    public Processable<Aligner<SchemaElementType, SchemaElementType>> runLabelBasedSchemaMatching(
            DataSet<SchemaElementType, SchemaElementType> schema,
            IComparator<SchemaElementType, SchemaElementType> labelComparator, double similarityThreshold)
            throws Exception {

        SymmetricBlocker<SchemaElementType, SchemaElementType, SchemaElementType, SchemaElementType> blocker = new NoSchemaBlocker<>();

        LinearCombinationMatchingRule<SchemaElementType, SchemaElementType> rule = new LinearCombinationMatchingRule<>(
                similarityThreshold);
        rule.addComparator(labelComparator, 1.0);

        RuleBasedDuplicateDetectionAlgorithm<SchemaElementType, SchemaElementType> algorithm = new RuleBasedDuplicateDetectionAlgorithm<SchemaElementType, SchemaElementType>(
                schema, rule, blocker);

        algorithm.setTaskName("Schema Matching");

        algorithm.run();

        return algorithm.getResult();
    }

    public Processable<Aligner<RecordType, SchemaElementType>> runDuplicateDetection(
            DataSet<RecordType, SchemaElementType> dataset,
            Processable<? extends Aligner<SchemaElementType, ? extends Matchable>> processable,
            MatchingRule<RecordType, SchemaElementType> rule,
            SymmetricBlocker<RecordType, SchemaElementType, RecordType, SchemaElementType> blocker) {

        RuleBasedDuplicateDetectionAlgorithm<RecordType, SchemaElementType> algorithm = new RuleBasedDuplicateDetectionAlgorithm<>(
                dataset, Aligner.toMatchable(processable), rule, blocker);
        algorithm.setTaskName("Duplicate Detection");

        algorithm.run();

        return algorithm.getResult();
    }

    public Processable<Aligner<RecordType, SchemaElementType>> runDuplicateDetection(
            DataSet<RecordType, SchemaElementType> dataset, MatchingRule<RecordType, SchemaElementType> rule,
            SymmetricBlocker<RecordType, SchemaElementType, RecordType, SchemaElementType> blocker) {

        RuleBasedDuplicateDetectionAlgorithm<RecordType, SchemaElementType> algorithm = new RuleBasedDuplicateDetectionAlgorithm<>(
                dataset, rule, blocker);
        algorithm.setTaskName("Duplicate Detection");

        algorithm.run();

        return algorithm.getResult();
    }

    public Processable<Aligner<RecordType, MatchableValue>> runVectorBasedIdentityResolution(
            DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
            BlockingKeyGenerator<RecordType, MatchableValue, RecordType> blockingfunction1,
            BlockingKeyGenerator<RecordType, MatchableValue, RecordType> blockingfunction2,
            BlockingKeyIndexer.VectorCreationMethod vectorCreation, VectorSpaceSimilarity similarity, double similarityThreshold) {

        VectorSpaceIdentityResolutionAlgorithm<RecordType, SchemaElementType> algorithm = new VectorSpaceIdentityResolutionAlgorithm<>(
                dataset1, dataset2, blockingfunction1, blockingfunction2, vectorCreation, similarity,
                similarityThreshold);

        algorithm.run();

        return algorithm.getResult();

    }

    public Processable<Aligner<SchemaElementType, MatchableValue>> runInstanceBasedSchemaMatching(
            DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
            BlockingKeyGenerator<RecordType, MatchableValue, SchemaElementType> blockingfunction1,
            BlockingKeyGenerator<RecordType, MatchableValue, SchemaElementType> blockingfunction2,
            BlockingKeyIndexer.VectorCreationMethod vectorCreation, VectorSpaceSimilarity similarity, double similarityThreshold) {

        VectorSpaceInstanceBasedSchemaMatchingAlgorithm<RecordType, SchemaElementType> algorithm = new VectorSpaceInstanceBasedSchemaMatchingAlgorithm<>(
                dataset1, dataset2, blockingfunction1, blockingfunction2, vectorCreation, similarity,
                similarityThreshold);

        algorithm.run();

        return algorithm.getResult();

    }


    public Processable<Aligner<SchemaElementType, RecordType>> runDuplicateBasedSchemaMatching(
            DataSet<SchemaElementType, SchemaElementType> schema1,
            DataSet<SchemaElementType, SchemaElementType> schema2,
            Processable<? extends Aligner<RecordType, ? extends Matchable>> instanceAligner,
            VotingMatchingRule<SchemaElementType, RecordType> rule,
            TopKVotesAggregator<SchemaElementType, RecordType> voteFilter,
            AlignedAggregator<SchemaElementType, RecordType> voteAggregator,
            Blocker<SchemaElementType, SchemaElementType, SchemaElementType, RecordType> schemaBlocker) {

        DuplicateBasedMatchingAlgorithm<RecordType, SchemaElementType> algorithm = new DuplicateBasedMatchingAlgorithm<>(
                schema1, schema2, Aligner.toMatchable(instanceAligner), rule, voteFilter, voteAggregator,
                schemaBlocker);

        algorithm.run();

        return algorithm.getResult();
    }
}
