package com.integration.em.tables;

import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.util.HashSet;
import java.util.Set;

public class TblColumnStatistics {

    private double standardDeviation;
    private double distinctValues;
    private double average;
    private double kurtosis;
    private double skewness;
    private double variance;

    public TblColumnStatistics() {
    }

    public TblColumnStatistics(TblColumn c) {
        calculate(c);
    }
    public double getStandardDeviation() {
        return standardDeviation;
    }

    public void setStandardDeviation(double standardDeviation) {
        this.standardDeviation = standardDeviation;
    }

    public double getDistinctValues() {
        return distinctValues;
    }

    public void setDistinctValues(double distinctValues) {
        this.distinctValues = distinctValues;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public double getKurtosis() {
        return kurtosis;
    }

    public void setKurtosis(double kurtosis) {
        this.kurtosis = kurtosis;
    }

    public double getSkewness() {
        return skewness;
    }

    public void setSkewness(double skewness) {
        this.skewness = skewness;
    }

    public double getVariance() {
        return variance;
    }

    public void setVariance(double variance) {
        this.variance = variance;
    }

    private void calculate(TblColumn c) {

        SynchronizedDescriptiveStatistics statistics = new SynchronizedDescriptiveStatistics();
        statistics.setWindowSize(-1);
        for (TblRow r : c.getTable().getTblRowArrayList()) {
            if(r.get(c.getColumnIndex()) != null)
                statistics.addValue((double) r.get(c.getColumnIndex()));
        }

        Set<Double> setofDistinctValues = new HashSet<>();
        for (double value : statistics.getValues()) {
            setofDistinctValues.add(value);
        }

        this.setVariance(statistics.getVariance());
        this.setSkewness(statistics.getSkewness());
        this.setKurtosis(statistics.getKurtosis());
        this.setDistinctValues((double) setofDistinctValues.size());
        this.setStandardDeviation(statistics.getStandardDeviation());
        this.setAverage(statistics.getMean());

    }
}
