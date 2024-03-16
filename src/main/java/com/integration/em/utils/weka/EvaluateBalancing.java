package com.integration.em.utils.weka;

import weka.classifiers.AbstractClassifier;
import weka.classifiers.Classifier;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.evaluation.output.prediction.AbstractOutput;
import weka.core.Instances;
import weka.filters.Filter;

import java.util.Random;

public class EvaluateBalancing extends Evaluation{

    private Filter trainingDataFilter;

    public EvaluateBalancing(Instances data, Filter trainingDataFilter) throws Exception {
        super(data);
        this.trainingDataFilter = trainingDataFilter;
    }

    @Override
    public void crossValidateModel(Classifier classifier, Instances data, int numFolds, Random random,
                                   Object... forPredictionsPrinting) throws Exception {
        data = new Instances(data);
        data.randomize(random);
        if (data.classAttribute().isNominal()) {
            data.stratify(numFolds);
        }


        AbstractOutput classificationOutput = null;
        if (forPredictionsPrinting.length > 0) {
            classificationOutput = (AbstractOutput) forPredictionsPrinting[0];
            classificationOutput.setHeader(data);
            classificationOutput.printHeader();
        }

        for (int i = 0; i < numFolds; i++) {
            Instances train = data.trainCV(numFolds, i, random);
            train = Filter.useFilter(train, trainingDataFilter);
            setPriors(train);
            Classifier copiedClassifier = AbstractClassifier.makeCopy(classifier);
            copiedClassifier.buildClassifier(train);
            Instances test = data.testCV(numFolds, i);
            evaluateModel(copiedClassifier, test, forPredictionsPrinting);
        }
        m_NumFolds = numFolds;

        if (classificationOutput != null) {
            classificationOutput.printFooter();
        }
    }
}
