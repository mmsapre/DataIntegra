package com.integration.em.processing;

import com.integration.em.datatypes.ValueDetectionType;
import com.integration.em.detect.DetectType;
import com.integration.em.model.Attribute;
import com.integration.em.model.DataSet;
import com.integration.em.tables.ValueNormalizer;
import lombok.extern.slf4j.Slf4j;
import com.integration.em.model.Record;
import java.util.Map;
@Slf4j
public class DataSetNormalizer<RecordType extends Record> {


    public void normalizeDataset(DataSet<RecordType, Attribute> dataSet, DetectType typeDetector){
        for(Attribute att: dataSet.getSchema().get()){
            ValueDetectionType columnType = this.detectColumnType(dataSet, att, typeDetector);
            this.normalizeColumn(columnType, dataSet, att);
        }
        log.info("Type detection and normalization are done!");
    }

    public ValueDetectionType detectColumnType(DataSet<RecordType, Attribute> dataSet, Attribute att, DetectType typeDetector){

        String [] values = new String[dataSet.size()];
        int index = 0;
        for(RecordType record: dataSet.get()){
            values[index] = record.getValue(att);
            index++;
        }
        if(typeDetector != null){
            return (ValueDetectionType) typeDetector.detectTypeForColumn(values, att.getIdentifier());
        }
        else{
            log.error("No type detector defined!");
            return null;
        }

    }

    public void normalizeColumn(ValueDetectionType columntype, DataSet<RecordType, Attribute> dataSet, Attribute att){
        ValueNormalizer valueNormalizer = new ValueNormalizer();
        for(RecordType record: dataSet.get()){

            Object value = valueNormalizer.normalize(record.getValue(att), columntype.getType(), columntype.getUnitCategory());

            if(value != null){
                record.setValue(att, value.toString());
            }

        }

    }

    public void normalizeDataset(DataSet<RecordType, Attribute> dataSet, Map<Attribute, ValueDetectionType> columnTypeMapping) {
        for(Attribute att: dataSet.getSchema().get()){
            ValueDetectionType columnType = columnTypeMapping.get(att);
            this.normalizeColumn(columnType, dataSet, att);
        }
        log.info("Normalization done!");
    }
}
