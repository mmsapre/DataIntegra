package com.integration.em.tables;

import com.integration.em.utils.Q;

import java.io.Serializable;
import java.util.*;

public class TblSchema implements Serializable {

    private ArrayList<TblColumn> columns;
    private HashMap<String, TblColumn> columnsById;

    private Map<Set<TblColumn>, Set<TblColumn>> functionalDependencies;
    private Set<Set<TblColumn>> candidateKeys;

    public TblSchema() {
        columns = new ArrayList<>();
        columnsById = new HashMap<>();
        functionalDependencies = new HashMap<>();
        candidateKeys = new HashSet<>();
    }

    public void addColumn(TblColumn column) {
        columns.add(column);
        columnsById.put(column.getIdentifier(), column);
    }

    public void insertColumn(int index, TblColumn column) {
        for(TblColumn c : columns) {
            if(c.getColumnIndex()>=index) {
                c.setColumnIndex(c.getColumnIndex()+1);
            }
        }
        column.setColumnIndex(index);
        columns.add(index, column);
        columnsById.put(column.getIdentifier(), column);
    }

    public void removeColumn(TblColumn column) {
        Iterator<TblColumn> colIt = columns.iterator();
        while(colIt.hasNext()) {
            if(colIt.next().getColumnIndex()==column.getColumnIndex()) {
                colIt.remove();
            }
        }

        Set<Map.Entry<Set<TblColumn>, Set<TblColumn>>> oldMap = functionalDependencies.entrySet();
        functionalDependencies = new HashMap<>();
        for(Map.Entry<Set<TblColumn>, Set<TblColumn>> e : oldMap) {
            Set<TblColumn> det = new HashSet<>(Q.where(e.getKey(), (c)->columns.contains(c)));
            Set<TblColumn> dep = new HashSet<>(Q.where(e.getValue(), (c)->columns.contains(c)));
            functionalDependencies.put(det, dep);
        }


        Set<Set<TblColumn>> oldKeys = candidateKeys;
        candidateKeys = new HashSet<>();
        for(Set<TblColumn> key : oldKeys) {
            candidateKeys.add(new HashSet<>(Q.where(key, (c)->columns.contains(c))));
        }

        for(TblColumn c: columns) {
            if(c.getColumnIndex()>column.getColumnIndex()) {
                c.setColumnIndex(c.getColumnIndex()-1);
            }
        }

        updateIdentifiers();
    }

    public void updateIdentifiers() {
        columnsById.clear();
        for(TblColumn c: columns) {
            c.updateIdentifier();
            columnsById.put(c.getIdentifier(), c);
        }

        Set<Map.Entry<Set<TblColumn>, Set<TblColumn>>> oldMap = functionalDependencies.entrySet();
        functionalDependencies = new HashMap<>();
        for(Map.Entry<Set<TblColumn>, Set<TblColumn>> e : oldMap) {
            Set<TblColumn> det = new HashSet<>(e.getKey());
            Set<TblColumn> dep = new HashSet<>(e.getValue());
            functionalDependencies.put(det, dep);
        }

        // update candidate keys
        Set<Set<TblColumn>> oldKeys = candidateKeys;
        candidateKeys = new HashSet<>();
        for(Set<TblColumn> key : oldKeys) {
            candidateKeys.add(new HashSet<>(key));
        }
    }

    public TblColumn get(int index) {
        return columns.get(index);
    }

    public TblColumn getRecord(String id) {
        return columnsById.get(id);
    }

    public Collection<TblColumn> getRecords() {
        return columns;
    }

    public String format() {
        StringBuilder sb = new StringBuilder();

        for(TblColumn tc : columns) {
            sb.append(String.format("[%d] %s\t%s\t%s\n", tc.getColumnIndex(), tc.getDataType(), tc.getIdentifier(), tc.getHeader()));
        }

        return sb.toString();
    }

    public String format(int columnWidth) {
        StringBuilder sb = new StringBuilder();

        boolean first=true;
        for(TblColumn c : columns) {

            if(!first) {
                sb.append(" | ");
            }

            sb.append(padRight(c.getHeader(),columnWidth));

            first = false;
        }

        return sb.toString();
    }

    public String formatDataTypes(int columnWidth) {
        StringBuilder sb = new StringBuilder();

        boolean first=true;
        for(TblColumn c : columns) {

            if(!first) {
                sb.append(" | ");
            }

            sb.append(padRight(c.getDataType().toString(),columnWidth));

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

    public int getSize() {
        return columns.size();
    }

    public int indexOf(TblColumn tc) {
        for(int i = 0; i < getSize(); i++) {
            if(get(i)==tc) {
                return i;
            }
        }
        return -1;
    }

   
    public Map<Set<TblColumn>, Set<TblColumn>> getFunctionalDependencies() {
        return functionalDependencies;
    }

   
    public void setFunctionalDependencies(
            Map<Set<TblColumn>, Set<TblColumn>> functionalDependencies) {
        this.functionalDependencies = functionalDependencies;
    }

    
    public Collection<Set<TblColumn>> getCandidateKeys() {
        return candidateKeys;
    }

  
    public void setCandidateKeys(
            Set<Set<TblColumn>> candidateKeys) {
        this.candidateKeys = candidateKeys;
    }
}
