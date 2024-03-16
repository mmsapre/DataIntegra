package com.integration.em.model;

import com.integration.em.processing.ProcessableCollection;

import java.util.*;

public class HashDataSet<RecordType extends Matchable, SchemaElementType extends Matchable> extends ProcessableCollection<RecordType> implements DataSet<RecordType, SchemaElementType> {

    protected Map<String, RecordType> records;
    private HashDataSet<SchemaElementType, SchemaElementType> attributes;



    public HashDataSet() {
        records = new HashMap<>();
    }


    public HashDataSet(Collection<RecordType> recordTypes) {
        records = new HashMap<>();

        for(RecordType r : recordTypes) {
            add(r);
        }
    }

    @Override
    public RecordType getRecord(String identifier) {
        return records.get(identifier);
    }

    @Override
    public void add(RecordType record) {

        records.put(record.getIdentifier(), record);
    }
    @Override
    public int size() {
        return records.size();
    }

    @Override
    public Collection<RecordType> get() {

        return records.values();
    }
    @Override
    public SchemaElementType getAttribute(String identifier) {

        return attributes.getRecord(identifier);
    }

    @Override
    public RecordType getRandomRecord() {
        Random r = new Random();
        List<RecordType> allRecords = new ArrayList<>(records.values());
        int index = r.nextInt(allRecords.size());
        return allRecords.get(index);
    }

    @Override
    public void ClearRecords() {
        records.clear();
    }

    @Override
    public void addAttribute(SchemaElementType attribute) {

        if(attributes==null) {
            attributes = new HashDataSet<>();
        }
        attributes.add(attribute);
    }

    @Override
    public DataSet<SchemaElementType, SchemaElementType> getSchema() {
        return attributes;
    }

    @Override
    public void removeRecord(String identifier) {
        records.remove(identifier);
    }
}
