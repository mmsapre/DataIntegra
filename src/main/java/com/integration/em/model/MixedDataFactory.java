package com.integration.em.model;

public interface MixedDataFactory<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    abstract RecordType createInstanceForMixed(RecordGroup<RecordType, SchemaElementType> cluster);

}
