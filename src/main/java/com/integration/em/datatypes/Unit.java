package com.integration.em.datatypes;

import java.util.Collection;

public class Unit {

    private String name;
    private Collection<String> abbreviations;
    private double factor;
    private UnitCategory unitCategory;


    public String getName() {
        return name;
    }


    public void setName(String name) {
        this.name = name;
    }


    public Collection<String> getAbbreviations() {
        return abbreviations;
    }


    public void setAbbreviations(Collection<String> abbreviations) {
        this.abbreviations = abbreviations;
    }


    public double getFactor() {
        return factor;
    }


    public void setFactor(double factor) {
        this.factor = factor;
    }


    public UnitCategory getUnitCategory() {
        return unitCategory;
    }


    public void setUnitCategory(UnitCategory unitCategory) {
        this.unitCategory = unitCategory;
    }

}
