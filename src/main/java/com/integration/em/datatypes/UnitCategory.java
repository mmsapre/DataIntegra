package com.integration.em.datatypes;

import java.util.ArrayList;
import java.util.List;

public class UnitCategory {

    private String name;
    private List<Unit> units = new ArrayList<>();

    public UnitCategory(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }


    public void setName(String name) {
        this.name = name;
    }


    public List<Unit> getUnits() {
        return units;
    }


    public void setUnits(List<Unit> units) {
        this.units = units;
    }


    public void addUnit(Unit unit) {
        this.units.add(unit);
    }
}
