package com.integration.em.model;

import com.integration.em.processing.Processable;
import com.integration.em.tables.Pair;

import java.util.*;

public class RecordGroup<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    private Map<String, MixedDataSet<RecordType, SchemaElementType>> mixedDataSetMap;

    public RecordGroup() {
        mixedDataSetMap=new HashMap<>();
    }

    public void addRecord(String id, MixedDataSet<RecordType, SchemaElementType> mixedDataSet) {
        mixedDataSetMap.put(id, mixedDataSet);
    }


    public void mergeWith(RecordGroup<RecordType, SchemaElementType> recordGroup) {

        mixedDataSetMap.putAll(recordGroup.mixedDataSetMap);
    }

    public Set<String> getRecordIds() {
        return mixedDataSetMap.keySet();
    }

    public int getSize() {
        return mixedDataSetMap.size();
    }

    public Collection<RecordType> getRecords() {
        Collection<RecordType> result = new LinkedList<>();

        for (String id : mixedDataSetMap.keySet()) {
            DataSet<RecordType, SchemaElementType> ds = mixedDataSetMap.get(id);

            result.add(ds.getRecord(id));
        }

        return result;
    }

    public Collection<Pair<RecordType, MixedDataSet<RecordType, SchemaElementType>>> getRecordsWithDataSets() {
        Collection<Pair<RecordType, MixedDataSet<RecordType, SchemaElementType>>> result = new LinkedList<>();

        for (String id : mixedDataSetMap.keySet()) {
            MixedDataSet<RecordType, SchemaElementType> elementTypeMixedDataSet = mixedDataSetMap.get(id);
            RecordType record = elementTypeMixedDataSet.getRecord(id);
            result.add(new Pair<>(record, elementTypeMixedDataSet));
        }

        return result;
    }

    public Aligner<SchemaElementType, Matchable> getSchemaCorrespondenceForRecord(RecordType recordType, Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable, SchemaElementType schemaElementType) {
        return null;
    }
}
