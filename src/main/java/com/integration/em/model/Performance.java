package com.integration.em.model;

public class Performance {

    private int correct;
    private int created;
    private int correct_total;

    public Performance(int correct, int created, int correct_total) {
        this.correct = correct;
        this.created = created;
        this.correct_total = correct_total;
    }

    public double getPrecision() {
        if(created==0) {
            return 0.0;
        } else {
            return (double) correct / (double) created;
        }
    }

    public double getRecall() {
        if(correct_total==0) {
            return 0.0;
        } else {
            return (double) correct / (double) correct_total;
        }
    }

    public double getF1() {
        if(getPrecision()==0 || getRecall()==0) {
            return 0.0;
        } else {
            return (2 * getPrecision() * getRecall())
                    / (getPrecision() + getRecall());
        }
    }

    public int getNumberOfPredicted() {
        return created;
    }

    public int getNumberOfCorrectlyPredicted() {
        return correct;
    }

    public int getNumberOfCorrectTotal() {
        return correct_total;
    }

}
