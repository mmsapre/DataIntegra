package com.integration.em.processing;


import java.io.Serializable;

public class Group<KeyType,RecordType> implements Serializable {

    private KeyType key;
    private Processable<RecordType> records;


    public KeyType getKey() {
        return key;
    }

    public Processable<RecordType> getRecords() {
        return records;
    }

    public Group() {

    }

    public Group(KeyType key, Processable<RecordType> records) {
        this.key = key;
        this.records = records;
    }
}
