package com.integration.em.model;

import au.com.bytecode.opencsv.CSVWriter;
import com.integration.em.processing.Processable;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CSVAlignerFormatter {

    public <TypeA extends Matchable, TypeB extends Matchable> void writeCSV(File file,
                                                                            Processable<Aligner<TypeA, TypeB>> dataset) throws IOException {
        CSVWriter writer = new CSVWriter(new FileWriter(file));

        for (Aligner<TypeA, TypeB> record : dataset.get()) {
            String[] values = format(record);

            writer.writeNext(values);
        }

        writer.close();
    }

    public <TypeA extends Matchable, TypeB extends Matchable> String[] format(Aligner<TypeA, TypeB> record) {
        return new String[] { record.getFirstRecordType().getIdentifier(), record.getSecondRecordType().getIdentifier(),
                Double.toString(record.getSimilarityScore()) };
    }
}
