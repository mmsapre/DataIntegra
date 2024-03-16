package com.integration.em.rules;

import com.integration.em.model.*;
import com.integration.em.processing.Processable;

import java.io.File;
import java.io.IOException;
import com.integration.em.model.Record;

public interface LearningMatchingRule<RecordType extends Matchable, SchemaElementType extends Matchable> {

    public Record generateFeatures(RecordType record1, RecordType record2,
                                   Processable<Aligner<SchemaElementType, Matchable>> schemaAligner,
                                   FeatureVectorDataSet features);
    public 	FeatureVectorDataSet initialiseFeatures(RecordType record1, RecordType record2, Processable<? extends Aligner<SchemaElementType, ? extends Matchable>> schemaCorrespondences);

    public Performance learnParameters(FeatureVectorDataSet features);

    void exportModel(File location);
    void readModel(File location);

    void exportTrainingData(DataSet<RecordType, SchemaElementType> dataset1,
                            DataSet<RecordType, SchemaElementType> dataset2,
                            BestMatchStandard goldStandard, File file) throws IOException;
}
