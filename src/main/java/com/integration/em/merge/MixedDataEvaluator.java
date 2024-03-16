package com.integration.em.merge;

import com.integration.em.model.*;
import com.integration.em.processing.Processable;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;

@Slf4j
public class MixedDataEvaluator<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    private MixedDataStrategy<RecordType,SchemaElementType> mixedDataStrategy;

    private RecordGroupFactory<RecordType,SchemaElementType> groupFactory;

    public MixedDataEvaluator(MixedDataStrategy<RecordType, SchemaElementType> mixedDataStrategy) {
        this.mixedDataStrategy = mixedDataStrategy;
    }

    public MixedDataEvaluator(MixedDataStrategy<RecordType, SchemaElementType> mixedDataStrategy, RecordGroupFactory<RecordType, SchemaElementType> groupFactory) {
        this.mixedDataStrategy = mixedDataStrategy;
        this.groupFactory = groupFactory;
    }


    public double evaluate(MixedDataSet<RecordType, SchemaElementType> dataset, DataSet<RecordType, SchemaElementType> bestMatch, Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable) {

        int correctValues = 0;
        int totalValues = bestMatch.size()
                * mixedDataStrategy.getAttributesMix(null, alignerProcessable).size();
        HashMap<SchemaElementType, Integer> attributeCount = new HashMap<SchemaElementType, Integer>();
        for (AttributeMixedTask<RecordType, SchemaElementType> fusionTask : mixedDataStrategy.getAttributesMix(null, alignerProcessable)) {
            attributeCount.put(fusionTask.getSchemaElement(), 0);
        }

        for (RecordType record : bestMatch.get()) {
            RecordType fused = dataset.getRecord(record.getIdentifier());

            if (fused != null) {

                RecordGroup<RecordType, SchemaElementType> g = groupFactory.createRecordGroup();
                g.addRecord(record.getIdentifier(), dataset);

                for (AttributeMixedTask<RecordType, SchemaElementType> fusionTask : mixedDataStrategy.getAttributesMix(g, alignerProcessable)) {
                    EvaluationRule<RecordType, SchemaElementType> r = fusionTask.getEvaluationRule();

                    if (r.isEqual(fused, record, fusionTask.getSchemaElement())) {
                        correctValues++;
                        attributeCount.put(fusionTask.getSchemaElement(),
                                attributeCount.get(fusionTask.getSchemaElement()) + 1);
                    } else {
                        log.trace(String.format(
                                "Error in '%s': %s <> %s", fusionTask.getSchemaElement().getIdentifier(),
                                fused.toString(), record.toString()));
                    }
                }
            }
        }
        log.trace("Attribute-specific Accuracy:");
        for (AttributeMixedTask<RecordType, SchemaElementType> fusionTask : mixedDataStrategy.getAttributesMix(null, alignerProcessable)) {
            double acc = (double) attributeCount.get(fusionTask.getSchemaElement())
                    / (double) bestMatch.size();
            log.trace(String.format("	%s: %.2f", fusionTask.getSchemaElement().getIdentifier(), acc));

        }

        return (double) correctValues / (double) totalValues;
    }
}
