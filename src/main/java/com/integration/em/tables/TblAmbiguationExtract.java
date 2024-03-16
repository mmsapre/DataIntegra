package com.integration.em.tables;

import com.integration.em.utils.MapUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TblAmbiguationExtract {

    private final static Pattern bracketsPattern = Pattern.compile(".*\\(([^)]*)\\).*");
    private final static Pattern bracketsPattern2 = Pattern.compile("\\(([^)]*)\\)");

    public void removeDisambiguations(Collection<Tbl> tables) {
        for(Tbl t : tables) {

            for(TblRow r : t.getTblRowArrayList()) {

                for(TblColumn c : t.getColumns()) {

                    Object value = r.get(c.getColumnIndex());
                    if(value!=null && !value.getClass().isArray()) { // ignore arrays for now

                        String stringValue = value.toString();
                        Matcher m = bracketsPattern.matcher(stringValue);

                        if(m.matches()) {
                            // remove the disambiguation from the cell value
                            stringValue = bracketsPattern2.matcher(stringValue).replaceAll("").trim();
                            if(stringValue.trim().isEmpty()) {
                                stringValue = null;
                            }
                            r.set(c.getColumnIndex(), stringValue);
                        }

                    }

                }

            }
        }
    }

    public Map<Integer, Map<Integer, TblColumn>> extractDisambiguations(Collection<Tbl> tables
    ) {

        // maps table id -> column id -> disambiguation column
        Map<Integer, Map<Integer, TblColumn>> tableToColumnToDisambiguation = new HashMap<>();

        for(Tbl t : tables) {

            Map<TblColumn, Map<TblRow, String>> disambiguations = new HashMap<>();

            // collect all disambiguations
            for(TblRow r : t.getTblRowArrayList()) {

                for(TblColumn c : t.getColumns()) {

                    Object value = r.get(c.getColumnIndex());
                    if(value!=null && !value.getClass().isArray()) { // ignore arrays for now

                        String stringValue = value.toString();
                        Matcher m = bracketsPattern.matcher(stringValue);

                        if(m.matches()) {

                            String disambiguation = m.group(1);

                            Map<TblRow, String> innerMap = MapUtils.get(disambiguations, c, new HashMap<>());
                            innerMap.put(r, disambiguation);

                            // remove the disambiguation from the cell value
                            stringValue = bracketsPattern2.matcher(stringValue).replaceAll("").trim();
                            if(stringValue.trim().isEmpty()) {
                                stringValue = null;
                            }
                            r.set(c.getColumnIndex(), stringValue);
                        }

                    }

                }

            }

            Map<TblColumn, TblColumn> newColumns = new HashMap<>();

            // decide which new columns to create
            for(TblColumn c : disambiguations.keySet()) {
                Map<TblRow, String> values = disambiguations.get(c);

                double percentDisambiguated = values.size() / (double)t.getTblRowArrayList().size();

                if(percentDisambiguated>=0.05) {
                    TblColumn newCol = createDisambiguationColumn(c);

                    t.insertColumn(newCol.getColumnIndex(), newCol);

                    newColumns.put(c, newCol);

                    Map<Integer, TblColumn> columnToDisambiguation = MapUtils.get(tableToColumnToDisambiguation, t.getTblId(), new HashMap<>());
                    columnToDisambiguation.put(c.getColumnIndex(), newCol);
                }

            }

            // fill new columns with values
            for(TblRow r : t.getTblRowArrayList()) {

                for(TblColumn c : disambiguations.keySet()) {

                    TblColumn c2 = newColumns.get(c);

                    if(c2!=null) {

                        Map<TblRow, String> values = disambiguations.get(c);

                        String value = values.get(r);

                        if(value!=null) {

                            r.set(c2.getColumnIndex(), value);

                        }

                    }
                }

            }

        }

        return tableToColumnToDisambiguation;
    }

    public TblColumn createDisambiguationColumn(TblColumn forColumn) {
        TblColumn newCol = new TblColumn(forColumn.getTable().getColumns().size(), forColumn.getTable());
        newCol.setDataType(forColumn.getDataType());

        if(forColumn.getHeader()!=null && !"".equals(forColumn.getHeader())) {
            newCol.setHeader(String.format("Disambiguation of %s", forColumn.getHeader()));
        }

        return newCol;
    }

}
