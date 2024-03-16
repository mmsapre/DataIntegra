package com.integration.em.model;

import java.time.LocalDateTime;
import java.util.Map;

public interface MixedDataSet<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> extends
        DataSet<RecordType, SchemaElementType> {

    void addOriginalId(RecordType record, String id);

    double getScore();

    void setScore(double score);

    LocalDateTime getDate();

    void setDate(LocalDateTime date);

    double getDensity();

    int getNumberOfValues(RecordType record);


    int getNumberOfAttributes(RecordType record);


    Map<SchemaElementType, Double> getAttributeDensities();


    void printDataSetDensityReport();

}
