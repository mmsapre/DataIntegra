package com.integration.em.similarity;

import java.io.Serializable;

public abstract class Similarity<DataType> implements Serializable {

    public abstract double calculate(DataType first, DataType second);

}
