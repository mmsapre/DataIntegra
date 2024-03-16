package com.integration.em.rules;

import com.integration.em.model.*;
import com.integration.em.processing.Processable;
import com.integration.em.rules.compare.IComparator;
import com.integration.em.rules.compare.IComparatorLogger;
import com.integration.em.tables.Pair;
import com.integration.em.utils.Q;
import com.integration.em.utils.TblStringUtils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import com.integration.em.model.Record;

public class LinearCombinationMatchingRule<RecordType extends Matchable, SchemaElementType extends Matchable> extends FilteringMatchingRule<RecordType, SchemaElementType>
        implements LearningMatchingRule<RecordType, SchemaElementType> {

    private List<Pair<IComparator<RecordType, SchemaElementType>, Double>> comparators;
    private double offset;

    public LinearCombinationMatchingRule(double finalThreshold) {
        super(finalThreshold);
        comparators = new LinkedList<>();
    }

    public LinearCombinationMatchingRule(double offset, double finalThreshold) {
        this(finalThreshold);
        this.offset = offset;
    }

    public LinearCombinationMatchingRule(double finalThreshold, List<Pair<IComparator<RecordType, SchemaElementType>, Double>> comparators, double offset) {
        super(finalThreshold);
        this.comparators = comparators;
        this.offset = offset;
    }




    public void addComparator(IComparator<RecordType, SchemaElementType> comparator, double weight) throws Exception {
        if (weight > 0.0) {
            comparators.add(new Pair<IComparator<RecordType, SchemaElementType>, Double>(comparator, weight));
            if (this.isDebugReportActive()) {
                comparator.setComparisonLog(new IComparatorLogger());
                addComparatorToLog(comparator);
            }
        } else {
            throw new Exception("Weight cannot be 0.0 or smaller");
        }
    }

    public void normalizeWeights() {
        Double sum = 0.0;
        for (Pair<IComparator<RecordType, SchemaElementType>, Double> pair : comparators) {
            sum += pair.getSecond();
        }
        List<Pair<IComparator<RecordType, SchemaElementType>, Double>> normComparators = new LinkedList<>();
        for (Pair<IComparator<RecordType, SchemaElementType>, Double> pair : comparators) {
            normComparators.add(new Pair<IComparator<RecordType, SchemaElementType>, Double>(pair.getFirst(),
                    (pair.getSecond() / sum)));
        }
        comparators = normComparators;
    }

    @Override
    public Record generateFeatures(RecordType record1, RecordType record2, Processable<Aligner<SchemaElementType, Matchable>> schemaAligner, FeatureVectorDataSet features) {
        Record model = new Record(String.format("%s-%s", record1.getIdentifier(), record2.getIdentifier()),
                this.getClass().getSimpleName());

        double sum = 0.0;
        Record debug = null;
        if (this.isDebugReportActive() && this.continueCollectDebugResults()) {
            debug = initializeDebugRecord(record1, record2, -1);
        }

        for (int i = 0; i < comparators.size(); i++) {
            Pair<IComparator<RecordType, SchemaElementType>, Double> pair = comparators.get(i);

            IComparator<RecordType, SchemaElementType> comp = pair.getFirst();

            if (this.isDebugReportActive()) {
                comp.getComparisonLog().initialise();
            }

            double similarity = comp.compare(record1, record2, null);

            String name = String.format("[%d] %s", i, comp.getClass().getSimpleName());
            Attribute att = null;
            for (Attribute elem : features.getSchema().get()) {
                if (elem.toString().equals(name)) {
                    att = elem;
                }
            }
            if (att == null) {
                att = new Attribute(name);
            }
            model.setValue(att, Double.toString(similarity));

            if (this.isDebugReportActive() && this.continueCollectDebugResults()) {
                debug = fillDebugRecord(debug, comp, i);
                addDebugRecordShort(record1, record2, comp, i);
            }

        }

        double similarity = offset + sum;
        if (this.isDebugReportActive() && this.continueCollectDebugResults()) {
            fillSimilarity(debug, similarity);
        }

        return model;
    }

    @Override
    public FeatureVectorDataSet initialiseFeatures(RecordType record1, RecordType record2, Processable<? extends Aligner<SchemaElementType, ? extends Matchable>> schemaCorrespondences) {
        FeatureVectorDataSet features = new FeatureVectorDataSet();

        for (int i = 0; i < comparators.size(); i++) {
            Pair<IComparator<RecordType, SchemaElementType>, Double> pair = comparators.get(i);

            IComparator<RecordType, SchemaElementType> comp = pair.getFirst();

            String name = String.format("[%d] %s", i, comp.getClass().getSimpleName());
            Attribute att = new Attribute(name);

            features.addAttribute(att);
        }

        return features;
    }

    @Override
    public Performance learnParameters(FeatureVectorDataSet features) {
        return null;
    }

    @Override
    public void exportModel(File location) {

    }

    @Override
    public void readModel(File location) {

    }

    @Override
    public void exportTrainingData(DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, BestMatchStandard goldStandard, File file) throws IOException {

        RuleLearner<Record, Attribute> learner = new RuleLearner<>();

        @SuppressWarnings("unchecked")
        FeatureVectorDataSet features = learner.generateTrainingDataForLearning((DataSet<Record, Attribute>) dataset1,
                (DataSet<Record, Attribute>) dataset2, goldStandard, (LearningMatchingRule<Record, Attribute>) this,
                null);
        new RecordCSVFormatter().writeCSV(file, features, null);
    }

    @Override
    public double compare(RecordType record1, RecordType record2, Aligner<SchemaElementType, Matchable> schemaAligner) {
        return 0;
    }

    @Override
    public Aligner<RecordType, SchemaElementType> apply(RecordType record1, RecordType record2, Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable) {

        double sum = 0.0;
        Record debug = null;
        if (this.isDebugReportActive() && this.continueCollectDebugResults()) {
            debug = initializeDebugRecord(record1, record2, -1);
        }
        for (int i = 0; i < comparators.size(); i++) {
            Pair<IComparator<RecordType, SchemaElementType>, Double> pair = comparators.get(i);

            IComparator<RecordType, SchemaElementType> comp = pair.getFirst();

            Aligner<SchemaElementType, Matchable> correspondence = getAlignerForComparator(
                    alignerProcessable, record1, record2, comp);

            if (this.isDebugReportActive()) {
                comp.getComparisonLog().initialise();
            }

            double similarity = comp.compare(record1, record2, correspondence);
            double weight = pair.getSecond();
            sum += (similarity * weight);

            if (this.isDebugReportActive() && this.continueCollectDebugResults()) {
                debug = fillDebugRecord(debug, comp, i);
                addDebugRecordShort(record1, record2, comp, i);
            }
        }


        double similarity = offset + sum;
        if (this.isDebugReportActive() && this.continueCollectDebugResults()) {
            fillSimilarity(debug, similarity);
        }


        return new Aligner<RecordType, SchemaElementType>(record1, record2, similarity, alignerProcessable);

    }

    @Override
    public String toString() {
        return String.format("LinearCombinationMatchingRule: %f + %s", offset,
                TblStringUtils.join(Q.project(comparators, (c) -> c.getSecond() + " " + c.getFirst().toString()), " + "));
    }

}
