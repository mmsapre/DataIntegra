package com.integration.em.datatypes;


public class ValueDetectionType extends ColumnType {
    
    private Quantity quantity;
    private UnitCategory unitCategory;
    
    public ValueDetectionType(DataType type, Quantity quantity, Unit unit, UnitCategory unitCategory) {
        super(type, unit);
        this.quantity = quantity;
        this.unitCategory = unitCategory;   
    }
    
    public ValueDetectionType(DataType type, UnitCategory unitCategory) {
        super(type, null);
        this.unitCategory = unitCategory;   
    }
    

    public Quantity getQuantity() {
        return quantity;
    }
    

    public UnitCategory getUnitCategory() {
        return unitCategory;
    }
    
}
