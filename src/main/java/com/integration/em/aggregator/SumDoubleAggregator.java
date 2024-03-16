package com.integration.em.aggregator;

public class SumDoubleAggregator<KeyType> extends SumAggregator<KeyType, Double> {

    public  Double getValue(Double record){
        return record;
    }



}
