package com.integration.em.model;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.integration.em.tables.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
@Slf4j
public class BestMatchStandard implements Serializable {

    private List<Pair<String, String>> positiveSamples;
    private List<Pair<String, String>> negativeSamples;
    private Set<String> canonicalPositiveSamples;
    private Set<String> canonicalNegativeSamples;
    private boolean isComplete = false;


    public BestMatchStandard() {
        positiveSamples = new LinkedList<>();
        negativeSamples = new LinkedList<>();
        canonicalPositiveSamples = new HashSet<>();
        canonicalNegativeSamples = new HashSet<>();
    }

    public List<Pair<String, String>> getPositiveSamples() {
        return positiveSamples;
    }

    public List<Pair<String, String>> getNegativeSamples() {
        return negativeSamples;
    }

    public Set<String> getCanonicalPositiveSamples() {
        return canonicalPositiveSamples;
    }

    public Set<String> getCanonicalNegativeSamples() {
        return canonicalNegativeSamples;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void loadFromCSVFile(File file) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(file));
        readAllLines(reader);
        reader.close();
        printGSReport();
    }

    public void writeToCSVFile(File file) throws IOException {
        CSVWriter writer = new CSVWriter(new FileWriter(file));

        writeAllLines(writer);

        writer.close();
    }


    private void readAllLines(CSVReader reader) throws IOException {
        String[] values = null;

        while ((values = reader.readNext()) != null) {

            if (values.length == 3) {
                boolean isPositive = Boolean.parseBoolean(values[2]);
                Pair<String, String> example = new Pair<String, String>(
                        values[0], values[1]);

                if (isPositive) {
                    addPositiveExample(example);
                } else {
                    addNegativeExample(example);
                }

            } else {
                log.error(String.format("Skipping malformed line: %s",
                        StringUtils.join(values,",")));
            }
        }
    }

    private void writeAllLines(CSVWriter writer) {
        String[] values = null;

        for(Pair<String, String> p : getPositiveSamples()) {
            values = new String[] { p.getFirst(), p.getSecond(), "true" };
            writer.writeNext(values);
        }
        for(Pair<String, String> p : getNegativeSamples()) {
            values = new String[] { p.getFirst(), p.getSecond(), "false" };
            writer.writeNext(values);
        }
    }


    public void loadFromTSVFile(File file) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(file),  '\t');
        readAllLines(reader);
        reader.close();
        printGSReport();

    }

    public void addPositiveExample(Pair<String, String> example) {
        positiveSamples.add(example);
        canonicalPositiveSamples.add(getCanonicalSamples(example.getFirst(),
                example.getSecond()));
    }

    public void addNegativeExample(Pair<String, String> example) {
        negativeSamples.add(example);
        canonicalNegativeSamples.add(getCanonicalSamples(example.getFirst(),
                example.getSecond()));
    }



    public boolean containsNegative(String id1, String id2) {
        String c = getCanonicalSamples(id1, id2);

        return canonicalNegativeSamples.contains(c);
    }

    public boolean containsNegative(Matchable record1, Matchable record2) {
        String c = getCanonicalSamples(record1, record2);

        return canonicalNegativeSamples.contains(c);
    }
    public void writeToTSVFile(File file) throws IOException {
        CSVWriter writer = new CSVWriter(new FileWriter(file), '\t');
        writeAllLines(writer);
        writer.close();
    }

    public void addPositiveSamples(Pair<String, String> example) {
        positiveSamples.add(example);
        canonicalPositiveSamples.add(getCanonicalSamples(example.getFirst(),
                example.getSecond()));
    }


    public void addNegativeSamples(Pair<String, String> example) {
        negativeSamples.add(example);
        canonicalNegativeSamples.add(getCanonicalSamples(example.getFirst(),
                example.getSecond()));
    }

    public boolean containsPositive(String id1, String id2) {
        String c = getCanonicalSamples(id1, id2);

        return canonicalPositiveSamples.contains(c);
    }

    private String getCanonicalSamples(String id1, String id2) {
        String first, second;

        if (id1.compareTo(id2) <= 0) {
            first = id1;
            second = id2;
        } else {
            first = id2;
            second = id1;
        }

        return first + "|" + second;
    }

    private String getCanonicalSamples(Matchable record1, Matchable record2) {
        String first, second;

        if (record1.getIdentifier().compareTo(record2.getIdentifier()) <= 0) {
            first = record1.getIdentifier();
            second = record2.getIdentifier();
        } else {
            first = record2.getIdentifier();
            second = record1.getIdentifier();
        }

        return first + "|" + second;
    }

    public void printGSReport() {
        int numPositive = getPositiveSamples().size();
        int numNegative = getNegativeSamples().size();
        int ttl = numPositive + numNegative;
        double positivePerCent = (double) numPositive / (double) ttl * 100.0;
        double negativePerCent = (double) numNegative / (double) ttl * 100.0;

        log.info(String
                .format("The gold standard has %d examples", ttl));
        log.info(String
                .format("\t%d positive examples (%.2f%%)",
                        numPositive, positivePerCent));
        log.info(String
                .format("\t%d negative examples (%.2f%%)",
                        numNegative, negativePerCent));

        // check for duplicates
        if (getPositiveSamples().size() != canonicalPositiveSamples.size()) {
            log.warn("The gold standard contains duplicate positive examples!");
        }
        if (getNegativeSamples().size() != canonicalNegativeSamples.size()) {
            log.warn("The gold standard contains duplicate negative examples!");
        }

        // check if any example was labeled as positive and negative
        HashSet<String> allExamples = new HashSet<>();
        allExamples.addAll(canonicalPositiveSamples);
        allExamples.addAll(canonicalNegativeSamples);

        if (allExamples.size() != (canonicalPositiveSamples.size() + canonicalNegativeSamples.size())) {
            log.warn("The gold standard contains an example that is both labelled as positive and negative!");
        }

    }

    public void printBalanceReport() {
        int numPositive = getPositiveSamples().size();
        int numNegative = getNegativeSamples().size();
        int ttl = numPositive + numNegative;
        double positivePerCent = (double) numPositive / (double) ttl;
        double negativePerCent = (double) numNegative / (double) ttl;

        if (Math.abs(positivePerCent - negativePerCent) > 0.2) {
            log.warn("The gold standard is imbalanced!");
        }
    }
}
