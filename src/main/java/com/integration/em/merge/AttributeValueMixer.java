package com.integration.em.merge;

import com.integration.em.cluster.ConnectComponentCluster;
import com.integration.em.model.*;
import com.integration.em.processing.Processable;
import com.integration.em.tables.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

@Slf4j
public abstract class AttributeValueMixer<ValueType, RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> extends AttributeMixer<RecordType, SchemaElementType> {

    private ConflictResolutionFunction<ValueType, RecordType, SchemaElementType> conflictResolution;

    public AttributeValueMixer(ConflictResolutionFunction<ValueType, RecordType, SchemaElementType> conflictResolution) {
        this.conflictResolution = conflictResolution;
    }

    protected List<MixValue<ValueType, RecordType, SchemaElementType>> getMixValue(RecordGroup<RecordType, SchemaElementType> group, Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable, SchemaElementType schemaElement) {
        List<MixValue<ValueType, RecordType, SchemaElementType>> values = new LinkedList<>();

        for(Pair<RecordType, MixedDataSet<RecordType, SchemaElementType>> p : group.getRecordsWithDataSets()) {
            RecordType record = p.getFirst();
            Aligner<SchemaElementType, Matchable> correspondence = group.getSchemaCorrespondenceForRecord(p.getFirst(), alignerProcessable, schemaElement);
            if(hasValue(record, correspondence)) {
                ValueType v = getValue(record, correspondence);
                MixValue<ValueType, RecordType, SchemaElementType> value = new MixValue<ValueType, RecordType, SchemaElementType>(v, p.getFirst(), p.getSecond());
                values.add(value);
            }
        }

        return values;
    }

    @Override
    public Double getConsistency(RecordGroup<RecordType, SchemaElementType> group, EvaluationRule<RecordType, SchemaElementType> rule, Processable<Aligner<SchemaElementType, Matchable>> schemaCorrespondences, SchemaElementType schemaElement) {

        List<RecordType> records = new ArrayList<>(group.getRecords());

        // remove non-existing values
        Iterator<RecordType> it = records.iterator();
        while(it.hasNext()) {
            RecordType record = it.next();
            Aligner<SchemaElementType, Matchable> aligner = group.getSchemaCorrespondenceForRecord(record, schemaCorrespondences, schemaElement);

            if(!hasValue(record, aligner)) {
                it.remove();
            }
        }

        if(records.size()==0) {
            return null;
        } else if(records.size()==1) {
            return 1.0; // this record group is consistent, as there is only one value
        }

        ConnectComponentCluster<RecordType> con = new ConnectComponentCluster<>();

        for(int i=0; i<records.size();i++) {
            RecordType r1 = records.get(i);
            Aligner<SchemaElementType, Matchable> aligner1 = group.getSchemaCorrespondenceForRecord(r1, schemaCorrespondences, schemaElement);

            for(int j=i+1; j<records.size(); j++) {
                RecordType r2 = records.get(j);
                Aligner<SchemaElementType, Matchable> cor2 = group.getSchemaCorrespondenceForRecord(r2, schemaCorrespondences, schemaElement);

                if(!con.isEdgeAlreadyInCluster(r1, r2)) {

                    Aligner<SchemaElementType, Matchable> aligner = null;
                    if(aligner1!=null && cor2!=null) {

                        aligner = Aligner.<SchemaElementType, Matchable>combine(aligner1, cor2);
                    }

                    if(rule.isEqual(r1, r2, aligner)) {
                        con.addEdge(new Triple<>(r1, r2, 1.0));
                    }

                }
            }
            // }
        }

        Map<Collection<RecordType>, RecordType> clusters = con.createResult();
        int largestClusterSize = 0;
        for(Collection<RecordType> cluster : clusters.keySet()) {
            if(cluster.size()>largestClusterSize) {
                largestClusterSize = cluster.size();
            }
        }

        if(largestClusterSize>group.getSize()) {
            log.error("Wrong cluster!");
        }

        return (double)largestClusterSize / (double)records.size();
    }



    protected MixedValue<ValueType, RecordType, SchemaElementType> getMixedValue(RecordGroup<RecordType, SchemaElementType> group, Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable, SchemaElementType schemaElement) {
        List<MixValue<ValueType, RecordType, SchemaElementType>> mixValues = getMixValue(group, alignerProcessable, schemaElement);

        MixedValue<ValueType, RecordType, SchemaElementType> mixedValueInstance = conflictResolution.resolveConflict(mixValues);

        if(this.isDebugResults() && mixedValueInstance.getValue() != null){

            List<String> listIdentifiers = new LinkedList<String>();
            List<String> listValues = new LinkedList<String>();

            if(mixValues.size() > 0){

                Collections.sort(mixValues, MixValue.Comparators.RECORDIDENTIFIER);

                for(int i = 0; i < mixValues.size(); i++){
                    listIdentifiers.add(mixValues.get(i).getRecord().getIdentifier().toString());
                    listValues.add(mixValues.get(i).getValue().toString());
                }
            }
            else{
                listIdentifiers.addAll(group.getRecordIds());
            }
            String recordIdentifiers = StringUtils.join(listIdentifiers, '+');

            String valueIdentifier = schemaElement.getIdentifier() + "-{" +  recordIdentifiers + "}";
            String values = "{" + StringUtils.join(listValues, '|') + "}";

            String fusedValue = mixedValueInstance.getValue().toString();

            AttributeMixedLogger attributeMixedLogger = new AttributeMixedLogger(valueIdentifier);
            attributeMixedLogger.setValueIDS(valueIdentifier);
            attributeMixedLogger.setRecordIDS(recordIdentifiers);
            attributeMixedLogger.setValue(values);
            attributeMixedLogger.setMixedValue(fusedValue);

            this.setAttributeMixedLogger(attributeMixedLogger);
        }


        return mixedValueInstance;
    }
    public abstract ValueType getValue(RecordType record, Aligner<SchemaElementType, Matchable> aligner);

}
