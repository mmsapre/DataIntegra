package com.integration.em.model;

import au.com.bytecode.opencsv.CSVWriter;
import com.integration.em.processing.Processable;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@Slf4j
public abstract class CSVFormatter<RecordType> {

    public void writeCSV(File file, Processable<RecordType> dataset) throws IOException {
        CSVWriter writer = new CSVWriter(new FileWriter(file));

        String[] headers = getHeader();
        if (headers != null) {
            writer.writeNext(headers);
        }

        for (RecordType record : dataset.get()) {
            String[] values = format(record);

            writer.writeNext(values);
        }

        writer.close();
    }
    public abstract String[] getHeader();

    public abstract String[] format(RecordType record);

}
