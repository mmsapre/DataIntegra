package com.integration.em.model;

public interface Matchable {

    String getIdentifier();


    String getProvenance();


    default int getDataSourceIdentifier() {
        return 0;
    }
}
