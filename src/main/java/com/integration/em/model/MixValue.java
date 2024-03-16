package com.integration.em.model;

import java.time.LocalDateTime;
import java.util.Comparator;

public class MixValue<ValueType, RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    private ValueType value;
    private RecordType record;

    private MixedDataSet<RecordType, SchemaElementType> mixedDataSet;

    public MixValue(ValueType value, RecordType record, MixedDataSet<RecordType, SchemaElementType> mixedDataSet) {
        this.value = value;
        this.record = record;
        this.mixedDataSet = mixedDataSet;
    }

    public ValueType getValue() {
        return value;
    }

    public RecordType getRecord() {
        return record;
    }

    public MixedDataSet<RecordType, SchemaElementType> getMixedDataSet() {
        return mixedDataSet;
    }

    public double getDataSourceScore() {
        return mixedDataSet.getScore();
    }

    public LocalDateTime getDateSourceDate() {
        return mixedDataSet.getDate();
    }

    public static class Comparators {
        public static final Comparator<MixValue> RECORDIDENTIFIER =
                (MixValue o1, MixValue o2) -> o1.getRecord().getIdentifier().compareTo(o2.getRecord().getIdentifier());
    }

}
