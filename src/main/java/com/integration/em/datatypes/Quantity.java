package com.integration.em.datatypes;

import java.util.Collection;


public class Quantity {
    
    private String name;
    private Collection<String> abbreviations;
    private double factor;

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
    
}
