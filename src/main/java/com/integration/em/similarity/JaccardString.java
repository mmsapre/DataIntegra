package com.integration.em.similarity;

import com.wcohen.ss.api.Token;
import com.wcohen.ss.tokens.SimpleTokenizer;

import java.util.LinkedList;
import java.util.List;

public class JaccardString extends Similarity<String>{

    private Similarity<String> stringSimilarity;
    private double threshold;

    private double jaccardThreshold;

    public JaccardString(Similarity<String> stringSimilarity, double threshold, double jaccardThreshold) {
        setStringSimilarity(stringSimilarity);
        setThreshold(threshold);
        setJaccardThreshold(jaccardThreshold);
    }

    public double getJaccardThreshold() {
        return jaccardThreshold;
    }

    public void setJaccardThreshold(double jaccardThreshold) {
        this.jaccardThreshold = jaccardThreshold;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public Similarity<String> getStringSimilarity() {
        return stringSimilarity;
    }

    public void setStringSimilarity(Similarity<String> stringSimilarity) {
        this.stringSimilarity = stringSimilarity;
    }

    @Override
    public double calculate(String first, String second) {
        SimpleTokenizer tok = new SimpleTokenizer(true, true);

        List<String> f = new LinkedList<>();
        List<String> s = new LinkedList<>();

        if(first!=null) {
            for(Token t : tok.tokenize(first)) {
                f.add(t.getValue());
            }
        }

        if(second!=null) {
            for(Token t : tok.tokenize(second)) {
                s.add(t.getValue());
            }
        }
        GLJaccard<String> j = new GLJaccard<>(getStringSimilarity(), getThreshold());
        double sim = j.calculate(f, s);

        return sim >= getJaccardThreshold() ? sim : 0.0;
    }
}
