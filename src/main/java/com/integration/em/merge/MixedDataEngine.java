package com.integration.em.merge;

import com.integration.em.model.*;
import com.integration.em.processing.Processable;
import com.integration.em.utils.ProgressReporter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MixedDataEngine<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    private MixedDataStrategy<RecordType, SchemaElementType> mixedDataStrategy;


    public MixedDataEngine(MixedDataStrategy<RecordType, SchemaElementType> mixedDataStrategy) {
        this.mixedDataStrategy = mixedDataStrategy;
    }

    public MixedDataStrategy<RecordType, SchemaElementType> getMixedDataStrategy() {
        return mixedDataStrategy;
    }

    public MixedDataSet<RecordType, SchemaElementType> run(AlignerSet<RecordType, SchemaElementType> alignerSet,
                                                           Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable) {
        MixedDataSet<RecordType, SchemaElementType> mixedDataSet = mixedDataStrategy.createMixedDataSet();
        for (RecordGroup<RecordType, SchemaElementType> clu : alignerSet.getRecordGroups()) {

            RecordType fusedRecord = mixedDataStrategy.apply(clu, alignerProcessable);

            mixedDataSet.add(fusedRecord);
            for (RecordType record : clu.getRecords()) {
                mixedDataSet.addOriginalId(fusedRecord, record.getIdentifier());
            }
        }


        return mixedDataSet;
    }

    public Map<String, Double> getAttributeConsistencies(AlignerSet<RecordType, SchemaElementType> alignerSet, Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable) {
        Map<String, Double> consistencySums = new HashMap<>();
        Map<String, Integer> consistencyCounts = new HashMap<>();

        ProgressReporter progress = new ProgressReporter(alignerSet.getRecordGroups().size(), "Calculating consistencies");

        for (RecordGroup<RecordType, SchemaElementType> clu : alignerSet.getRecordGroups()) {

            Map<String, Double> values = mixedDataStrategy.getAttributeConsistency(clu, alignerProcessable);

            for (String att : values.keySet()) {
                Double consistencyValue = values.get(att);

                if(consistencyValue!=null) {
                    Integer cnt = consistencyCounts.get(att);
                    if (cnt == null) {
                        cnt = 0;
                    }
                    consistencyCounts.put(att, cnt + 1);

                    Double sum = consistencySums.get(att);
                    if(sum == null) {
                        sum = 0.0;
                    }
                    consistencySums.put(att, sum + consistencyValue);
                }
            }

            progress.incrementProgress();
            progress.report();
        }

        Map<String, Double> result = new HashMap<>();
        for (String att : consistencySums.keySet()) {
            if(consistencySums.get(att)!=null) {
                double consistency = consistencySums.get(att)
                        / (double) consistencyCounts.get(att);

                result.put(att, consistency);
            }
        }

        return result;
    }

}
