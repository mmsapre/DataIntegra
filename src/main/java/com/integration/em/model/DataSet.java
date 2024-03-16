package com.integration.em.model;

import com.integration.em.processing.Processable;

public interface DataSet<RecordType extends Matchable, SchemaElementType extends Matchable> extends Processable<RecordType> {

    RecordType getRecord(String identifier);

    SchemaElementType getAttribute(String identifier);


    RecordType getRandomRecord();


    void ClearRecords();

    void addAttribute(SchemaElementType attribute);

    DataSet<SchemaElementType, SchemaElementType> getSchema();

    void removeRecord(String identifier);
}
