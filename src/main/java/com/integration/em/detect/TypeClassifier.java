package com.integration.em.detect;

import com.integration.em.datatypes.ColumnType;
import com.integration.em.datatypes.DataType;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.AnnotationPipeline;
import edu.stanford.nlp.pipeline.POSTaggerAnnotator;
import edu.stanford.nlp.pipeline.TokenizerAnnotator;
import edu.stanford.nlp.pipeline.WordsToSentencesAnnotator;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.Tokenizer;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.time.TimeAnnotator;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TypeClassifier implements DetectType{

    private static AnnotationPipeline pipeline;
    private Classifier classifier;
    private Features features;

    public TypeClassifier() {
        pipeline = new AnnotationPipeline();
        classifier = new Classifier();
        features = new Features(new MaxentTagger(
                "\\english-left3words-distsim.tagger"));
        initialize();
    }

    public void initialize() {
        Properties props = new Properties();
        pipeline.addAnnotator(new TokenizerAnnotator(false) {

            @Override
            public Tokenizer<CoreLabel> getTokenizer(Reader r) {
                return new PTBTokenizer<CoreLabel>(r, new CoreLabelTokenFactory(), "");

            }

        });
        pipeline.addAnnotator(new WordsToSentencesAnnotator(false));
        pipeline.addAnnotator(new POSTaggerAnnotator(false));
        pipeline.addAnnotator(new TimeAnnotator("sutime", props));
    }

    @Override
    public ColumnType detectTypeForColumn(Object[] attributeValues, String attributeLabel) {
        List<String> features = new ArrayList<String>();

        features = calculateFeatures((String[]) attributeValues);
        DataType type = null;
        try {
            type = predictDatatype(features);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ColumnType resColumnType = new ColumnType(type, null);
        return resColumnType;
    }

    public DataType predictDatatype(List<String> calculatedFeatures)
            throws IOException {

        DataType type = classifier.classify(calculatedFeatures);

        return type;
    }

    public List<String> calculateFeatures(String[] col) {

        List<String> featuresList = new ArrayList<String>();

        features.createFeatures(col, pipeline);

        featuresList.add(String
                .valueOf(features.getPercentageofAlphabeticCharacters()));
        featuresList.add(String
                .valueOf(features.getPercentageofPunctuationCharacters()));
        featuresList.add(String.valueOf(features.getCellContentPattern()));
        featuresList.add(String.valueOf(
                features.isContainPunctuationCharactersinHeaderCell()));
        featuresList.add(String.valueOf(features.getPOSPatternofHeaderCell()));
        featuresList.add(String.valueOf(features.getAverageCharacterLenghth()));
        featuresList.add(String.valueOf(features.isIsDateorTime()));
        featuresList.add(String.valueOf(features.isIsBooleanValue()));

        return featuresList;
    }
}
