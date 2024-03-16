package com.integration.em.model;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class CSVDataSetFormatter<RecordType extends Matchable, SchemaElementType extends Matchable> {

    public abstract String[] getHeader(List<SchemaElementType> orderedHeader);

    public abstract String[] format(RecordType record, DataSet<RecordType, SchemaElementType> dataset,
                                    List<SchemaElementType> orderedHeader);


    public void writeCSV(File file, DataSet<RecordType, SchemaElementType> dataset, List<SchemaElementType> orderedHeader)
            throws IOException {
        CSVWriter writer = new CSVWriter(new FileWriter(file));

        String[] headers = null;
        if(orderedHeader != null){
            headers = getHeader(orderedHeader);
        }
        else{
            headers = getHeader(sortAttributesAlphabetically(dataset));
        }


        if (headers != null) {
            writer.writeNext(headers);
        }

        for (RecordType record : dataset.get()) {
            String[] values = format(record, dataset, orderedHeader);

            writer.writeNext(values);
        }

        writer.close();
    }


    private List<SchemaElementType> sortAttributesAlphabetically(DataSet<RecordType, SchemaElementType> dataset) {
        List<SchemaElementType> attributes = new ArrayList<>();

        for (SchemaElementType elem : dataset.getSchema().get()) {
            attributes.add(elem);
        }

        Collections.sort(attributes, new Comparator<SchemaElementType>() {

            @Override
            public int compare(SchemaElementType o1, SchemaElementType o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });

        return attributes;
    }
}
