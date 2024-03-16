package com.integration.em.rules;

import com.integration.em.model.*;
import com.integration.em.processing.ParallelProcessableCollection;
import com.integration.em.processing.Processable;
import com.integration.em.tables.Pair;
import com.integration.em.utils.ProgressReporter;
import com.integration.em.utils.Q;
import edu.stanford.nlp.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import com.integration.em.model.Record;

@Slf4j
public class RuleLearner<RecordType extends Matchable, SchemaElementType extends Matchable> {

    public Performance learnMatchingRule(DataSet<RecordType, SchemaElementType> data1,
                                         DataSet<RecordType, SchemaElementType> data2,
                                         Processable<? extends Aligner<SchemaElementType, ?>> schemaCorrespondences,
                                         LearningMatchingRule<RecordType, SchemaElementType> rule, BestMatchStandard trainingData,
                                         boolean deduplicateTrainingData) {

        FeatureVectorDataSet features = generateTrainingDataForLearning(data1, data2, trainingData, rule,
                schemaCorrespondences);

        if (deduplicateTrainingData) {
            features = deduplicateFeatureDataSet(features);
        }

        return rule.learnParameters(features);
    }

    public Performance learnMatchingRule(DataSet<RecordType, SchemaElementType> data1,
                                         DataSet<RecordType, SchemaElementType> data2,
                                         Processable<? extends Aligner<SchemaElementType, ?>> schemaCorrespondences,
                                         LearningMatchingRule<RecordType, SchemaElementType> rule, BestMatchStandard trainingData) {
        return learnMatchingRule(data1, data2, schemaCorrespondences, rule, trainingData, false);
    }

    public FeatureVectorDataSet deduplicateFeatureDataSet(FeatureVectorDataSet features) {

        FeatureVectorDataSet deduplicated = new FeatureVectorDataSet();

        List<Attribute> orderedAttributes = new LinkedList<>();
        for (Attribute a : features.getSchema().get()) {
            orderedAttributes.add(a);
            deduplicated.addAttribute(a);
        }

        for (Record r : features.get()) {

            String id = StringUtils.join(Q.project(orderedAttributes, (a) -> r.getValue(a)));

            Record uniqueRecord = new Record(id);

            for (Attribute a : orderedAttributes) {
                uniqueRecord.setValue(a, r.getValue(a));
            }

            deduplicated.add(uniqueRecord);

        }

        log.info(String.format("Deduplication removed %d/%d examples.", features.size() - deduplicated.size(),
                features.size()));

        return deduplicated;
    }


    public FeatureVectorDataSet generateTrainingDataForLearning(DataSet<RecordType, SchemaElementType> dataset1,
                                                                DataSet<RecordType, SchemaElementType> dataset2, BestMatchStandard bestMatchStandard,
                                                                LearningMatchingRule<RecordType, SchemaElementType> rule,
                                                                Processable<? extends Aligner<SchemaElementType, ? extends Matchable>> processable) {
        LocalDateTime start = LocalDateTime.now();

        FeatureVectorDataSet result = rule.initialiseFeatures(dataset1.getRandomRecord(), dataset2.getRandomRecord(), processable);

        bestMatchStandard.printBalanceReport();

        log.info(String.format("Starting GenerateFeatures", start.toString()));

        ProgressReporter progress = new ProgressReporter(
                bestMatchStandard.getPositiveSamples().size() + bestMatchStandard.getNegativeSamples().size(),
                "GenerateFeatures");

        Processable<Record> positiveExamples = new ParallelProcessableCollection<>(bestMatchStandard.getPositiveSamples())
                .map((Pair<String, String> correspondence) -> {
                    RecordType record1 = dataset1.getRecord(correspondence.getFirst());
                    RecordType record2 = dataset2.getRecord(correspondence.getSecond());

                    if (record1 == null && record2 == null) {

                        record1 = dataset2.getRecord(correspondence.getFirst());
                        record2 = dataset1.getRecord(correspondence.getSecond());
                    }


                    if (record1 != null && record2 != null) {
                        Record features = rule.generateFeatures(record1, record2,
                                Aligner.toMatchable(processable), result);
                        features.setValue(FeatureVectorDataSet.ATTRIBUTE_LABEL, "1");
                        return features;
                    } else {
                        return null;
                    }
                });

        Processable<Record> negativeExamples = new ParallelProcessableCollection<>(bestMatchStandard.getNegativeSamples())
                .map((Pair<String, String> correspondence) -> {
                    RecordType record1 = dataset1.getRecord(correspondence.getFirst());
                    RecordType record2 = dataset2.getRecord(correspondence.getSecond());

                    if (record1 == null && record2 == null) {

                        record1 = dataset2.getRecord(correspondence.getFirst());
                        record2 = dataset1.getRecord(correspondence.getSecond());
                    }


                    if (record1 != null && record2 != null) {
                        Record features = rule.generateFeatures(record1, record2,
                                Aligner.toMatchable(processable), result);
                        features.setValue(FeatureVectorDataSet.ATTRIBUTE_LABEL, "0");

                        return features;
                    } else {
                        return null;
                    }
                });

        for (Record r : positiveExamples.get()) {
            result.add(r);
        }
        for (Record r : negativeExamples.get()) {
            result.add(r);
        }


        LocalDateTime end = LocalDateTime.now();

        log.info(String.format("GenerateFeatures finished after %s; created %,d examples.",
                DurationFormatUtils.formatDurationHMS(Duration.between(start, end).toMillis()), result.size()));

        return result;
    }
}
