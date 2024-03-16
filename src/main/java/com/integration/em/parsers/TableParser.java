package com.integration.em.parsers;

import com.integration.em.detect.DetectType;
import com.integration.em.detect.RowContentDetect;
import com.integration.em.detect.TblHeaderDetect;
import com.integration.em.tables.ListHandler;
import com.integration.em.tables.Tbl;
import com.integration.em.tables.TblRow;
import org.apache.commons.lang.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;

public abstract class TableParser {

    private boolean cleanHeader = true;
    private DetectType detectType;

    private TblHeaderDetect tblHeaderDetect;

    private StringNormalizer stringNormalizer;

    private RowContentDetect rowContentDetect;

    private boolean convertValues = true;

    public boolean isConvertValues() {
        return convertValues;
    }

    public void setConvertValues(boolean convertValues) {
        this.convertValues = convertValues;
    }

    public boolean isCleanHeader() {
        return cleanHeader;
    }

    public void setCleanHeader(boolean cleanHeader) {
        this.cleanHeader = cleanHeader;
    }

    public DetectType getDetectType() {
        return detectType;
    }

    public void setDetectType(DetectType detectType) {
        this.detectType = detectType;
    }

    public TblHeaderDetect getTblHeaderDetect() {
        return tblHeaderDetect;
    }

    public void setTblHeaderDetect(TblHeaderDetect tblHeaderDetect) {
        this.tblHeaderDetect = tblHeaderDetect;
    }

    public StringNormalizer getStringNormalizer() {
        return stringNormalizer;
    }

    public void setStringNormalizer(StringNormalizer stringNormalizer) {
        this.stringNormalizer = stringNormalizer;
    }

    public RowContentDetect getRowContentDetect() {
        return rowContentDetect;
    }

    public void setRowContentDetect(RowContentDetect rowContentDetect) {
        this.rowContentDetect = rowContentDetect;
    }

    public void populateTable(String[][] tContent, Tbl t, int[] skipRows) {
        int tableRowIndex = 0;
        for (int rowIdx = 0; rowIdx < tContent.length; rowIdx++) {
            if (!ArrayUtils.contains(skipRows, rowIdx)) {
                String[] rowData = tContent[rowIdx];
                Object[] values = new Object[tContent[rowIdx].length];
                for (int i = 0; i < rowData.length && i < values.length; i++) {
                    if (rowData[i] != null && !rowData[i].trim().isEmpty()) {

                        if(ListHandler.checkIfList(rowData[i])) {
                            List<String> listValues = new LinkedList<>();
                            for(String v : ListHandler.splitList(rowData[i])) {
                                v = stringNormalizer.normaliseValue(rowData[i], false);

                                if (!((String) v).equalsIgnoreCase(StringNormalizer.nullValue)) {
                                } else {
                                    listValues.add(v);
                                }
                            }
                            values[i] = listValues.toArray();
                        } else {
                            values[i] = stringNormalizer.normaliseValue(rowData[i], false);

                            if (((String) values[i]).equalsIgnoreCase(StringNormalizer.nullValue)) {
                                values[i] = null;
                            } else {
                                values[i] = values[i];
                            }
                        }

                    }
                }

                TblRow r = new TblRow(tableRowIndex, t);
                tableRowIndex++;
                r.set(values);
                t.addRow(r);
            }
        }
    }

    public abstract Tbl parseTable(File file);
    public abstract Tbl parseTable(Reader reader, String fileName) throws IOException;
}
