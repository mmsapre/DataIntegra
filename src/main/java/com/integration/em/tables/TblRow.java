package com.integration.em.tables;

import java.util.*;

public class TblRow {

    private Object[] values;
    private int rowNumber;
    private Tbl table;
    private List<String> provenance;
    public TblRow() {
        super();
    }

    public TblRow(int rowNumber, Tbl table) {
        this.table = table;
        this.rowNumber = rowNumber;
        provenance = new LinkedList<>();
    }

    public String getIdentifier() {
        //return String.format("%s~Row%s", table.getPath(), rowNumber);
        return formatRowIdentifier(table.getPath(), rowNumber);
    }

    public static String formatRowIdentifier(String tableName, int rowNumber) {
        return String.format("%s~Row%s", tableName, rowNumber);
    }

    public void set(int columnIndex, Object value) {
        values[columnIndex] = value;
    }
    public Object get(int columnIndex) {
        if(values==null || columnIndex>=values.length) {
            return null;
        } else {
            return values[columnIndex];
        }
    }
    public void set(Object[] values) {
        this.values = values;
    }
    public Object[] getValueArray() {
        return values;
    }

    public int getRowNumber() {
        if(table==null) {
            return -1;
        } else {
            if(rowNumber==-1) {
                rowNumber = table.getTblRowArrayList().indexOf(this);
            }
            return rowNumber;
        }
    }


    public void invalidateRowNumber() {
        rowNumber=-1;
    }

    protected void setRowNumber(int number) {
        rowNumber = number;
    }

    public Tbl getTbl() {
        return table;
    }

    public Object getKeyValue() {
        if(table.hasSubjectColumn()) {
            return get(table.getSubjectColumnIndex());
        } else {
            return null;
        }
    }


    public List<String> getProvenance() {
        return provenance;
    }


    public void setProvenance(List<String> provenance) {
        this.provenance = provenance;
    }

    public String format(int columnWidth) {
        StringBuilder sb = new StringBuilder();

        // sort columns for output
        ArrayList<TblColumn> columns = new ArrayList<>(table.getColumns());
        Collections.sort(columns, new TblColumn.TblColumnByIndexComparator());

        boolean first=true;
        for(TblColumn c : columns) {

            if(!first) {
                sb.append(" | ");
            }

            Object v = get(c.getColumnIndex());
            String value = v==null ? "null" : v.toString();
            sb.append(padRight(value,columnWidth));

            first = false;
        }

        return sb.toString();
    }

    protected String padRight(String s, int n) {
        if(n==0) {
            return "";
        }
        if (s.length() > n) {
            s = s.substring(0, n);
        }
        s = s.replace("\n", " ");
        return String.format("%1$-" + n + "s", s);
    }

    public TblRow copy(Tbl t) {
        TblRow r = new TblRow(t.getTblRowArrayList().size(), t);
        r.set(getValueArray());
        r.setProvenance(getProvenance());
        r.addProvenanceForRow(this);
        return r;
    }


    @Override
    public boolean equals(Object obj) {
        if(obj instanceof TblRow) {
            TblRow r = (TblRow)obj;
            return getIdentifier().equals(r.getIdentifier()) ;
        }
        return super.equals(obj);
    }


    @Override
    public int hashCode() {
        return getIdentifier().hashCode();
    }

    public Object[] project(Collection<TblColumn> projectedColumns) {
        Map<Integer, Integer> columnIndexProjection = new HashMap<>();

        // project the table schema
        int idx = 0;
        for(int i = 0; i < table.getColumns().size(); i++) {
            TblColumn c = table.getTblSchema().get(i);

            if(projectedColumns.contains(c)) {
                columnIndexProjection.put(i, idx++);
            }
        }

        Object[] oldValues = getValueArray();
        Object[] newValues = new Object[projectedColumns.size()];

        for(int i = 0; i < oldValues.length; i++) {
            if(columnIndexProjection.containsKey(i)) {
                newValues[columnIndexProjection.get(i)] = oldValues[i];
            }
        }

        return newValues;
    }

    public void addProvenanceForRow(TblRow row) {
        if(row.getProvenance().size()>0) {
            getProvenance().addAll(row.getProvenance());
        } else {
            getProvenance().add(row.getIdentifier());
        }
    }
}
