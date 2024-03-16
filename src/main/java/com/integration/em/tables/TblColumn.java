package com.integration.em.tables;


import com.integration.em.datatypes.DataType;
import com.integration.em.datatypes.Unit;
import com.integration.em.utils.Func;

import java.io.Serializable;
import java.util.*;

public class TblColumn implements Serializable, Comparable<TblColumn>{

    private DataType dataType;
    private String header;
    private int columnIndex;
    private Tbl table;
    private String uri;
    private Unit unit;
    private List<String> provenance;
    private Collection<String> synonyms;
    private String identifier=null;

    public TblColumn() {
        provenance = new LinkedList<>();
        synonyms = new LinkedList<>();
    }

    public TblColumn(int columnIndex, Tbl table) {
        this.columnIndex = columnIndex;
        this.table = table;
        provenance = new LinkedList<>();
        synonyms = new LinkedList<>();
        updateIdentifier();
    }

    public void addProvenanceForColumn(TblColumn column) {
        if(column.getProvenance()!=null && column.getProvenance().size() > 0) {
            getProvenance().addAll(column.getProvenance());
            getProvenance().add(column.getIdentifier());
        } else {
            getProvenance().add(column.getIdentifier());
        }
    }

    public void updateIdentifier() {
        identifier = String.format("%s~Col%s", table.getPath(), columnIndex);
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getUniqueName() {
        return getIdentifier();
    }

    public String getHeader() {
        return header;
    }
    public void setHeader(String header) {
        this.header = header;
    }

    public DataType getDataType() {
        return dataType;
    }
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public int getColumnIndex() {
        return columnIndex;
    }
    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        updateIdentifier();
    }

    public Tbl getTable() {
        return table;
    }

    public String getUri() {
        return uri;
    }
    public void setUri(String uri) {
        this.uri = uri;
    }


    public Unit getUnit() {
        return unit;
    }

    public void setUnit(Unit unit) {
        this.unit = unit;
    }


    public List<String> getProvenance() {
        return provenance;
    }


    public void setProvenance(List<String> provenance) {
        this.provenance = provenance;
    }


    public Collection<String> getSynonyms() {
        return synonyms;
    }

    public void setSynonyms(Collection<String> synonyms) {
        this.synonyms = synonyms;
    }


    @Override
    public int compareTo(TblColumn o) {
        return getIdentifier().compareTo(o.getIdentifier());
    }


    @Override
    public boolean equals(Object obj) {
        if(obj instanceof TblColumn) {
            return getIdentifier().equals(((TblColumn) obj).getIdentifier());
        } else {
            return super.equals(obj);
        }
    }


    @Override
    public int hashCode() {
        return getIdentifier().hashCode();
    }

    public TblColumn copy(Tbl t, int columnIndex) {
        return copy(t, columnIndex, true);
    }

    public TblColumn copy(Tbl t, int columnIndex, boolean addProvenance) {
        TblColumn c = new TblColumn(columnIndex, t);
        c.setDataType(getDataType());
        c.setHeader(getHeader());
        c.setUnit(getUnit());
        c.setUri(getUri());
        c.setProvenance(new LinkedList<>(getProvenance()));
        if(addProvenance) {
            c.addProvenanceForColumn(this);
        }
        c.setSynonyms(new HashSet<>(getSynonyms()));
        return c;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return String.format("{%d}[%d] %s", getTable().getTblId(), getColumnIndex(), getHeader());
    }
    public static class ColumnIdentifierProjection implements Func<String, TblColumn> {
        @Override
        public String invoke(TblColumn in) {
            return in.getIdentifier();
        }

    }

    public static class ColumnHeaderProjection implements Func<String, TblColumn> {

        @Override
        public String invoke(TblColumn in) {
            return in.getHeader();
        }

    }

    public static class ColumnIndexProjection implements Func<Integer, TblColumn> {


        @Override
        public Integer invoke(TblColumn in) {
            return in.getColumnIndex();
        }

    }

    public static class ColumnIndexAndHeaderProjection implements Func<String, TblColumn> {

        private String regexToRemove;

        public ColumnIndexAndHeaderProjection() {
        }

        public ColumnIndexAndHeaderProjection(String regexToRemove) {
            this.regexToRemove = regexToRemove;
        }


        @Override
        public String invoke(TblColumn in) {
            if(regexToRemove==null) {
                return String.format("[%d]%s", in.getColumnIndex(), in.getHeader());
            } else {
                return String.format("[%d]%s", in.getColumnIndex(), in.getHeader()).replaceAll(regexToRemove, "");
            }
        }

    }

    public static class TblColumnByIndexComparator implements Comparator<TblColumn> {

        @Override
        public int compare(TblColumn record1, TblColumn record2) {
            return Integer.compare(record1.getColumnIndex(), record2.getColumnIndex());
        }
    }
    public TblColumnStatistics calculateColumnStatistics() {
        return new TblColumnStatistics(this);
    }
}
