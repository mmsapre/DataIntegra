package com.integration.em.datatypes;

public class ColumnType {

    private final DataType type;
    private final Unit unit;

    public ColumnType(DataType type, Unit unit) {
        this.type = type;
        this.unit = unit;
    }


    public DataType getType() {
        return type;
    }


    public Unit getUnit() {
        return unit;
    }
}
