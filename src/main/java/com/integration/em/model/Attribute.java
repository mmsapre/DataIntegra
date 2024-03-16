package com.integration.em.model;

import java.io.Serializable;

public class Attribute implements Matchable, Serializable {

    protected String identifier;
    protected String provenance;

    private String name;

    public Attribute() {
    }

    public Attribute(String identifier) {
        this.identifier = identifier;
    }

    public Attribute(String identifier, String provenance) {
        this.identifier = identifier;
        this.provenance = provenance;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getProvenance() {
        return provenance;
    }

    @Override
    public String toString() {
        return getIdentifier();
    }
}
