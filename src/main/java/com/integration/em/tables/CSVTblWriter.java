package com.integration.em.tables;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class CSVTblWriter implements TblWriter{
    @Override
    public File write(Tbl t, File f) throws Exception {
        if(!f.getName().endsWith(".csv")) {
            f = new File(f.getAbsolutePath() + ".csv");
        }

        return write(t,f,new CSVWriter(new FileWriter(f)));
    }

    public File write(Tbl t, File f, char separator, char quoteChar, char escapeChar) throws IOException {

        if(!f.getName().endsWith(".csv")) {
            f = new File(f.getAbsolutePath() + ".csv");
        }

        return write(t,f,new CSVWriter(new FileWriter(f), separator, quoteChar, escapeChar));
    }

    protected File write(Tbl t, File f, CSVWriter w) throws IOException {

        List<String> values = new LinkedList<>();

        for(TblColumn c : t.getColumns()) {
            values.add(c.getHeader());
        }
        w.writeNext(values.toArray(new String[values.size()]));

        // write values
        for(TblRow r : t.getTblRowArrayList()) {

            values.clear();

            for(TblColumn c: t.getColumns()) {

                Object value = r.get(c.getColumnIndex());

                if(value!=null) {
                    if(value.getClass().isArray()) {
                        List<String> listValues = new LinkedList<>();
                        for(Object v : (Object[])value) {
                            if(v!=null) {
                                listValues.add(v.toString());
                            }
                        }
                        values.add(ListHandler.formatList(listValues));
                    } else {
                        values.add(value.toString());
                    }
                } else {
                    values.add(null);
                }

            }

            w.writeNext(values.toArray(new String[values.size()]));
        }

        w.close();

        return f;
    }
}
