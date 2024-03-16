package com.integration.em.model;

import au.com.bytecode.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public abstract class CSVMatchableReader<RecordType extends Matchable, SchemaElementType extends Matchable> {

    public void loadFromCSV(File file, DataSet<RecordType, SchemaElementType> dataset) throws IOException {

        CSVReader reader = new CSVReader(new FileReader(file));

        String[] values = null;
        int rowNumber = 0;

        while((values = reader.readNext()) != null) {
            readLine(file, rowNumber++, values, dataset);
        }

        reader.close();

    }

    protected abstract void readLine(File file, int rowNumber, String[] values, DataSet<RecordType, SchemaElementType> dataset);

}
