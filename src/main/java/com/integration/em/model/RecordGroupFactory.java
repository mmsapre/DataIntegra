package com.integration.em.model;


public class RecordGroupFactory<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    public RecordGroup<RecordType, SchemaElementType> createRecordGroup() {
        return new RecordGroup<>();
    }

}
