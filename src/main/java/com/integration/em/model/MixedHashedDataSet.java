package com.integration.em.model;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MixedHashedDataSet<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> extends
        HashDataSet<RecordType, SchemaElementType> implements MixedDataSet<RecordType, SchemaElementType> {


    private double score;
    private LocalDateTime date;

    private Map<String, RecordType> originalIdIndex = new HashMap<>();

    @Override
    public void addOriginalId(RecordType recordType, String id) {
        originalIdIndex.put(id, recordType);
    }

    @Override
    public RecordType getRecord(String identifier) {
        RecordType record = super.getRecord(identifier);

        if (record == null) {
            record = originalIdIndex.get(identifier);
        }

        return record;
    }
    @Override
    public double getScore() {
        return score;
    }

    @Override
    public void setScore(double score) {
        this.score=score;
    }

    @Override
    public LocalDateTime getDate() {
        return date;
    }

    @Override
    public void setDate(LocalDateTime date) {
        this.date=date;
    }

    @Override
    public double getDensity() {
        int values = 0;
        int attributes = 0;

        for (RecordType record : get()) {
            values += getNumberOfValues(record);
            attributes += getNumberOfAttributes(record);
        }

        return (double) values / (double) attributes;
    }

    @Override
    public int getNumberOfValues(RecordType recordType) {
        int cnt = 0;
        for (SchemaElementType att : getSchema().get()) {
            cnt += recordType.hasValue(att) ? 1 : 0;
        }
        return cnt;
    }

    @Override
    public int getNumberOfAttributes(RecordType recordType) {
        return getSchema().size();
    }

    @Override
    public Map<SchemaElementType, Double> getAttributeDensities() {
        Map<SchemaElementType, Integer> sizes = new HashMap<>();
        Map<SchemaElementType, Integer> values = new HashMap<>();

        for (RecordType record : get()) {

            for (SchemaElementType att : getSchema().get()) {

                Integer size = sizes.get(att);
                if (size == null) {
                    size = 0;
                }
                sizes.put(att, size + 1);

                if (record.hasValue(att)) {
                    Integer value = values.get(att);
                    if (value == null) {
                        value = 0;
                    }
                    values.put(att, value + 1);
                }
            }

        }

        Map<SchemaElementType, Double> result = new HashMap<>();

        for (SchemaElementType att : sizes.keySet()) {
            Integer valueCount = values.get(att);
            if (valueCount == null) {
                valueCount = 0;
            }
            double density = (double) valueCount / (double) sizes.get(att);
            result.put(att, density);
        }

        return result;
    }

    @Override
    public void printDataSetDensityReport() {

        Map<SchemaElementType, Double> densities = getAttributeDensities();
        for (SchemaElementType att : densities.keySet()) {
            log.info(String.format("\t%s: %.2f", att.toString(),
                    densities.get(att)));
        }
    }
}
