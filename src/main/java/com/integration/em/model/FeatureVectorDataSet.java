package com.integration.em.model;

public class FeatureVectorDataSet extends MixedHashedDataSet<Record,Attribute>{

    public static final Attribute ATTRIBUTE_LABEL = new Attribute("label");

    public FeatureVectorDataSet() {
        super();
    }
}
