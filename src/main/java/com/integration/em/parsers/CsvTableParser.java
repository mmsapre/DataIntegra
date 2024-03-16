package com.integration.em.parsers;

import au.com.bytecode.opencsv.CSVReader;
import com.integration.em.datatypes.DataType;
import com.integration.em.detect.DetectType;
import com.integration.em.detect.InferType;
import com.integration.em.detect.SermiStructRowContentDetect;
import com.integration.em.detect.TblHeaderDetectFirstRow;
import com.integration.em.tables.Tbl;
import com.integration.em.tables.TblColumn;
import com.integration.em.tables.TblMapping;
import com.integration.em.utils.TblStringUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class CsvTableParser extends TableParser{

    public CsvTableParser() {
        setDetectType(new InferType());
        setTblHeaderDetect(new TblHeaderDetectFirstRow());
        setStringNormalizer(new DynaStringNormalizer());
        setRowContentDetect(new SermiStructRowContentDetect());
    }

    public CsvTableParser(DetectType detectType) {
        setDetectType(detectType);
        setTblHeaderDetect(new TblHeaderDetectFirstRow());
        setStringNormalizer(new DynaStringNormalizer());
        setRowContentDetect(new SermiStructRowContentDetect());
    }

    @Override
    public Tbl parseTable(File file) {
        Reader r = null;
        Tbl t = null;
        try {
            if (file.getName().endsWith(".gz")) {
                GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file));
                r = new InputStreamReader(gzip, "UTF-8");
            } else {
                r = new InputStreamReader(new FileInputStream(file), "UTF-8");
            }

            t = parseTable(r, file.getName());

            r.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return t;
    }

    @Override
    public Tbl parseTable(Reader reader, String fileName) throws IOException {
        Tbl tbl = new Tbl();
        tbl.setPath(fileName);

        TblMapping tm = new TblMapping();
        boolean typesAlreadyDetected = false;

        // read data
        List<String[]> tableListContent = null;

        try {
            // create reader
            CSVReader csvReader = new CSVReader(reader);

            tableListContent = csvReader.readAll();

            csvReader.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        // check whether table content is not empty!
        if (tableListContent == null)
            return null;

        // skip annotations
        // if the current line starts with #, check for valid annotations
        boolean isMetaData = tableListContent.get(0)[0].startsWith("#");

        while (isMetaData) {
            isMetaData = false;

            // check all valid annotations
            for (String s : TblMapping.VALID_ANNOTATIONS) {
                if (tableListContent.get(0)[0].startsWith(s)) {
                    isMetaData = true;
                    break;
                }
            }

            // if the current line is an annotation, read the next line and
            // start over
            if (isMetaData) {
                // join the values back together and let the metadata parser
                // handle the line
                tm.parseMetadata(TblStringUtils.join(tableListContent.get(0), ","));

                tableListContent.remove(0);
                isMetaData = tableListContent.get(0)[0].startsWith("#");
            }
        }

        int maxWidth = 0;
        for(String[] line : tableListContent) {
            maxWidth = Math.max(maxWidth, line.length);
        }

        // convert content into String[][] format for easier processing.
        String[][] tableContent = new String[tableListContent.size()][];
        tableListContent.toArray(tableContent);

        // make sure all rows have the same length!
        for(int i = 0; i < tableContent.length; i++) {
            if(tableContent[i].length<maxWidth) {
                tableContent[i] = Arrays.copyOf(tableContent[i], maxWidth);
            }
        }

        // close CSV Reader
        int[] emptyRowCount		=	getRowContentDetect().detectEmptyHeaderRows(tableContent, false);
        if(emptyRowCount==null) emptyRowCount = new int[] {};
        int[] headerRowCount 	= 	getTblHeaderDetect().detectTableHeader(tableContent, emptyRowCount);

        int colIdx = 0;
        // set the header, if possible
        if (headerRowCount != null) {

            if(tableContent.length > headerRowCount[0]) {
                for (String columnName : tableContent[headerRowCount[0]]) {
                    TblColumn c = new TblColumn(colIdx, tbl);

                    String header = columnName;
                    if (isCleanHeader()) {
                        header = this.getStringNormalizer().normaliseHeader(header);
                    }
                    c.setHeader(header);

                    if (tm.getDataType(colIdx) != null) {
                        c.setDataType((DataType) tm.getDataType(colIdx));
                        typesAlreadyDetected = true;
                    } else {
                        c.setDataType(DataType.unknown);
                    }

                    tbl.addColumn(c);

                    colIdx++;
                }
            }

        }

        //check for total row
        int[] sumRowCount	= 	getRowContentDetect().detectSumRow(tableContent);

        // populate table content
        int[] skipRows = ArrayUtils.addAll(emptyRowCount, headerRowCount);
        skipRows = ArrayUtils.addAll(skipRows, sumRowCount);
        populateTable(tableContent, tbl, skipRows);

        if (typesAlreadyDetected && isConvertValues()) {
            tbl.convertValues();
        } else if (isConvertValues()) {
            tbl.inferSchemaAndConvertValues(this.getDetectType());
        } else {
            tbl.inferSchema(this.getDetectType());
        }

        if (!tbl.hasSubjectColumn()) {
            tbl.identifySubjectColumn();
        }

        return tbl;

    }
}
