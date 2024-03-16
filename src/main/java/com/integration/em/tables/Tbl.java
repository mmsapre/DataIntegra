package com.integration.em.tables;

import com.integration.em.datatypes.ColumnType;
import com.integration.em.datatypes.DataType;
import com.integration.em.datatypes.TypeConverter;
import com.integration.em.detect.DetectType;
import com.integration.em.detect.InferType;
import com.integration.em.detect.TblKeyIdentification;
import com.integration.em.parallel.Consumer;
import com.integration.em.parallel.Parallel;
import com.integration.em.tables.*;
import com.integration.em.utils.MapUtils;
import com.integration.em.utils.Q;

import java.util.*;

public class Tbl {

    private ArrayList<TblRow> tblRowArrayList = new ArrayList<>();
    private String path;
    TblSchema tblSchema = new TblSchema();
    private int subjectColumnIndex = -1;
    private int TblId = 0;
    private TblMapping tblMapping;
    private TblContext tblContext;

    public Tbl(){

    }

    public ArrayList<TblRow> getTblRowArrayList() {
        return tblRowArrayList;
    }

    public void setTblRowArrayList(ArrayList<TblRow> tblRowArrayList) {
        this.tblRowArrayList = tblRowArrayList;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
        getTblSchema().updateIdentifiers();
    }

    public TblSchema getTblSchema() {
        return tblSchema;
    }

    public void setTblSchema(TblSchema tblSchema) {
        this.tblSchema = tblSchema;
    }

    public int getSubjectColumnIndex() {
        return subjectColumnIndex;
    }

    public void setSubjectColumnIndex(int subjectColumnIndex) {
        this.subjectColumnIndex = subjectColumnIndex;
    }

    public int getTblId() {
        return TblId;
    }

    public void setTblId(int TblId) {
        this.TblId = TblId;
    }

    public TblMapping getTblMapping() {
        if (tblMapping == null) {
            tblMapping = new TblMapping();
        }
        return tblMapping;
    }

    public void setTblMapping(TblMapping tblMapping) {
        this.tblMapping = tblMapping;
    }

    public TblContext getTblContext() {
        return tblContext;
    }

    public void setTblContext(TblContext tblContext) {
        this.tblContext = tblContext;
    }

    public boolean hasSubjectColumn() {
        return subjectColumnIndex >= 0;
    }

    public TblColumn getSubjectColumn() {
        if (hasSubjectColumn()) {
            return tblSchema.get(getSubjectColumnIndex());
        } else {
            return null;
        }
    }

    public Collection<TblColumn> getColumns() {
        return tblSchema.getRecords();
    }

    public int getSize() {
        return tblRowArrayList.size();
    }

    public void addColumn(TblColumn tblColumn) {
        tblSchema.addColumn(tblColumn);
    }

    public void addRow(TblRow tblRow) {
        tblRowArrayList.add(tblRow);
    }

    public void clear() {
        tblRowArrayList.clear();
        tblRowArrayList.trimToSize();
    }

    public void endLoad() {
        tblRowArrayList.trimToSize();
    }



    public TblRow get(int rowIndex) {
        if (tblRowArrayList.size() > rowIndex) {
            return tblRowArrayList.get(rowIndex);
        } else {
            return null;
        }
    }


    public void insertColumn(int index, TblColumn tblColumn) {

        if (subjectColumnIndex != -1 && index <= subjectColumnIndex) {
            subjectColumnIndex++;
        }

        if(getTblMapping().getMappedProperties()!=null) {
            Pair<String,Double>[] columnMapping = Arrays.copyOf(getTblMapping().getMappedProperties(), getTblMapping().getMappedProperties().length);
            for(int i = 0; i < getColumns().size(); i++) {
                if(i>=index && columnMapping.length>i) {
                    getTblMapping().setMappedProperty(i+1, columnMapping[i]);
                }
            }
            getTblMapping().setMappedProperty(index, null);
        }

        getTblSchema().insertColumn(index, tblColumn);

        for (TblRow r : getTblRowArrayList()) {
            Object[] oldValues = r.getValueArray();
            Object[] newValues = new Object[oldValues.length + 1];

            for (int i = 0; i < oldValues.length; i++) {
                if (i < index) {
                    newValues[i] = oldValues[i];
                } else if (i >= index) {
                    newValues[i + 1] = oldValues[i];
                }
            }

            r.set(newValues);
        }
    }

    public void removeColumn(TblColumn c) {
        if (subjectColumnIndex != -1 && c.getColumnIndex() <= subjectColumnIndex) {
            subjectColumnIndex--;
        } else if (c.getColumnIndex() == subjectColumnIndex) {
            subjectColumnIndex = -1;
        }

        getTblSchema().removeColumn(c);

        for (TblRow r : getTblRowArrayList()) {
            Object[] oldValues = r.getValueArray();
            Object[] newValues = new Object[oldValues.length - 1];

            for (int i = 0; i < newValues.length; i++) {
                if (i < c.getColumnIndex()) {
                    newValues[i] = oldValues[i];
                } else if (i >= c.getColumnIndex()) {
                    newValues[i] = oldValues[i + 1];
                }
            }

            r.set(newValues);
        }
    }

    public TblRow removeRow(int rowNumber) {
        return getTblRowArrayList().remove(rowNumber);
    }

    public void inferSchemaAndConvertValues() {

        inferSchema();
        convertValues();
    }

    public void inferSchemaAndConvertValues(DetectType detectType) {
        inferSchema(detectType);
        convertValues();
    }

    public void inferSchema() {
        final InferType tg = new InferType();

        inferSchema(tg);
    }

    public void inferSchema(final DetectType detectType) {
        try {
            Parallel.forLoop(0, getTblSchema().getSize(), new Consumer<Integer>() {

                @Override
                public void execute(Integer i) {
                    String attributeLabel = getTblSchema().get(i).getHeader();
                    String[] column = new String[getSize() + 1];

                    if (getTblSchema().get(i).getDataType() == DataType.unknown) {

                        // detect types and units per value
                        int rowCounter = 0;
                        column[rowCounter] = getTblSchema().get(i).getHeader();
                        rowCounter++;

                        boolean nullValues = true;
                        for (TblRow r : getTblRowArrayList()) {

                            if (nullValues == true && r.get(i) != null)
                                nullValues = false;
                            if(ListHandler.isArray(r.get(i))) {
                                Object[] array = ((Object[])r.get(i));
                                if(array.length>0) {
                                    column[rowCounter] = (String) array[0];
                                }
                            } else {
                                column[rowCounter] = (String) r.get(i);
                            }
                            rowCounter++;
                        }

                        ColumnType columnType = null;
                        if (!nullValues) {
                            columnType = detectType.detectTypeForColumn(column, attributeLabel);

                            if (columnType == null || columnType.getType() == null)
                                columnType = new ColumnType(DataType.string, null);

                        } else {
                            columnType = new ColumnType(DataType.string, null);
                        }

                        if (columnType.getType() == DataType.unit) {
                            getTblSchema().get(i).setDataType(DataType.number);
                            getTblSchema().get(i).setUnit(columnType.getUnit());
                        } else {
                            getTblSchema().get(i).setDataType(columnType.getType());
                        }

                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void convertValues() {
        TypeConverter tc = new TypeConverter();

        for (TblRow r : getTblRowArrayList()) {

            for (int i = 0; i < getTblSchema().getSize(); i++) {

                Object typedValue = null;

                if(ListHandler.isArray(r.get(i))) {

                    Object[] values = (Object[])r.get(i);
                    Object[] typedValues = new Object[values.length];

                    for(int j = 0; j < values.length; j++) {
                        typedValues[j] =tc.typeValue((String) values[j], getTblSchema().get(i).getDataType(), getTblSchema().get(i).getUnit());
                    }

                    typedValue = typedValues;

                } else {
                    typedValue = tc.typeValue((String) r.get(i), getTblSchema().get(i).getDataType(), getTblSchema().get(i).getUnit());
                }

                r.set(i, typedValue);

            }

        }
    }

    public void addDataTypesToMapping() {
        for(TblColumn c : getColumns()) {
            getTblMapping().setDataType(c.getColumnIndex(), c.getDataType());
        }
    }

    public void identifySubjectColumn() {
        identifySubjectColumn(0.3);
    }

    public void identifySubjectColumn(double uniquenessThreshold) {
        identifySubjectColumn(uniquenessThreshold, false);
    }

    public void identifySubjectColumn(double uniquenessThreshold, boolean verbose) {
        if (hasSubjectColumn()) {
            // reset existing subject column
            setSubjectColumnIndex(-1);
        }

        TblKeyIdentification tki = new TblKeyIdentification();

        tki.setKeyUniquenessThreshold(uniquenessThreshold);

        tki.identifyKeys(this);
    }

    public void append(Tbl t) {
        for (TblRow r : t.getTblRowArrayList()) {
            TblRow r2 = new TblRow(getTblRowArrayList().size(), this);
            r2.set(r.getValueArray());
            r2.addProvenanceForRow(r);
            addRow(r2);
        }
    }

    public void clearProvenance() {
        for (TblRow r : getTblRowArrayList()) {
            r.getProvenance().clear();
        }
        for (TblColumn c : getColumns()) {
            c.getProvenance().clear();
        }
    }

    public void reorganiseRowNumbers() {
        int number = 0;
        for (TblRow r : getTblRowArrayList()) {
            r.setRowNumber(number++);
        }
    }

    public Map<Integer, Integer> projectColumnIndices(Collection<TblColumn> projectedColumns) {
        Map<Integer, Integer> columnIndexProjection = new HashMap<>();

        // project the Tbl schema
        int idx = 0;
        for (int i = 0; i < getColumns().size(); i++) {
            TblColumn c = getTblSchema().get(i);

            if (projectedColumns.contains(c)) {
                columnIndexProjection.put(i, idx++);
            }
        }

        return columnIndexProjection;
    }

    public Map<Set<TblColumn>,Set<TblColumn>> projectFunctionalDependencies(Collection<TblColumn> projectedColumns) throws Exception {
        Map<Set<TblColumn>,Set<TblColumn>> result = new HashMap<>();

        // copy functional dependencies
        for(Pair<Set<TblColumn>,Set<TblColumn>> fd : Pair.fromMap(getTblSchema().getFunctionalDependencies())) {
            Set<TblColumn> det = fd.getFirst();

            Set<TblColumn> dep = fd.getSecond();
            Set<TblColumn> depIntersection = Q.intersection(projectedColumns,dep);
            if (projectedColumns.containsAll(det) && depIntersection.size()>0) {
                result.put(det, depIntersection);
            }
        }

        return result;
    }

    public Tbl project(Collection<TblColumn> projectedColumns) throws Exception {
        return project(projectedColumns, true);
    }

    public Tbl project(Collection<TblColumn> projectedColumns, boolean addProvenance) throws Exception {
        Tbl result = new Tbl();

        Map<Integer, Integer> columnIndexProjection = new HashMap<>();

        // project the Tbl schema
        int idx = 0;
        for (int i = 0; i < getColumns().size(); i++) {
            TblColumn c = getTblSchema().get(i);

            if (projectedColumns.contains(c)) {
                columnIndexProjection.put(i, idx);
                result.addColumn(c.copy(result, idx++, addProvenance));
            }
        }

        // copy path and key index
        if (columnIndexProjection.containsKey(getSubjectColumnIndex())) {
            result.setSubjectColumnIndex(columnIndexProjection.get(getSubjectColumnIndex()));
        }
        result.setPath(getPath());

        // copy functional dependencies
        for(Pair<Set<TblColumn>,Set<TblColumn>> fd : Pair.fromMap(getTblSchema().getFunctionalDependencies())) {
            Set<TblColumn> det = fd.getFirst();

            Set<TblColumn> dep = fd.getSecond();
            Set<TblColumn> depIntersection = Q.intersection(projectedColumns,dep);
            if (projectedColumns.containsAll(det) && depIntersection.size()>0) {
                Set<TblColumn> newDet = new HashSet<>();

                for (TblColumn c : det) {
                    newDet.add(result.getTblSchema().get(columnIndexProjection.get(c.getColumnIndex())));
                }

                Set<TblColumn> newDep = new HashSet<>();
                for (TblColumn c : depIntersection) {
                    newDep.add(result.getTblSchema().get(columnIndexProjection.get(c.getColumnIndex())));
                }

                result.getTblSchema().getFunctionalDependencies().put(newDet, newDep);
            }
        }

        // copy candidate keys
        for (Set<TblColumn> key : getTblSchema().getCandidateKeys()) {
            if (projectedColumns.containsAll(key)) {
                Set<TblColumn> newKey = new HashSet<>();
                for (TblColumn c : key) {
                    newKey.add(result.getTblSchema().get(columnIndexProjection.get(c.getColumnIndex())));
                }
                result.getTblSchema().getCandidateKeys().add(newKey);
            }
        }

        // fill the new Tbl with data
        for (TblRow r : getTblRowArrayList()) {
            Object[] oldValues = r.getValueArray();
            Object[] newValues = new Object[projectedColumns.size()];

            if(oldValues!=null) {
                for (int i = 0; i < oldValues.length; i++) {
                    if (columnIndexProjection.containsKey(i)) {
                        newValues[columnIndexProjection.get(i)] = oldValues[i];
                    }
                }
            }

            TblRow nr = new TblRow(r.getRowNumber(), result);
            nr.set(newValues);
            result.addRow(nr);
        }

        return result;
    }

    public Tbl join(Tbl otherTbl, Collection<Pair<TblColumn,TblColumn>> joinOn, Collection<TblColumn> projection) throws Exception {
        Map<TblColumn, TblColumn> inputColumnToOutputColumn = new HashMap<>();
        return join(otherTbl, joinOn, projection, inputColumnToOutputColumn);
    }

    public Tbl join(Tbl otherTbl, Collection<Pair<TblColumn,TblColumn>> joinOn, Collection<TblColumn> projection, Map<TblColumn, TblColumn> inputColumnToOutputColumn) throws Exception {

        // hash the join keys
        Map<TblColumn, Map<Object, Collection<TblRow>>> index = new HashMap<>();
        for(TblRow r : otherTbl.getTblRowArrayList()) {
            for(Pair<TblColumn, TblColumn> p : joinOn) {
                TblColumn joinKey = p.getSecond();
                Object value = r.get(joinKey.getColumnIndex());
                if(value!=null) {
                    Map<Object, Collection<TblRow>> columnValues = MapUtils.getFast(index, joinKey, (c)->new HashMap<Object,Collection<TblRow>>());
                    Collection<TblRow> rowsWithValue = MapUtils.getFast(columnValues, value, (o)->new LinkedList<TblRow>());
                    rowsWithValue.add(r);
                }
            }
        }

        // create the result Tbl
        Tbl result = project(Q.intersection(getColumns(), projection));
        result.getTblSchema().setFunctionalDependencies(new HashMap<>());
        result.getTblSchema().setCandidateKeys(new HashSet<>());
        result.clear();
        for(Map.Entry<Integer, Integer> translation : projectColumnIndices(Q.intersection(getColumns(), projection)).entrySet()) {
            inputColumnToOutputColumn.put(getTblSchema().get(translation.getKey()), result.getTblSchema().get(translation.getValue()));
        }
        Collection<TblColumn> otherColumns = Q.without(projection, getColumns());
        for(TblColumn c : otherColumns) {
            TblColumn out = new TblColumn(result.getColumns().size(), result);
            out.setDataType(c.getDataType());
            out.setHeader(c.getHeader());
            result.addColumn(out);
            inputColumnToOutputColumn.put(c, out);
        }

        // set the Tbl mapping - class
        Pair<String, Double> thisClass = getTblMapping().getMappedClass();
        Pair<String, Double> otherClass = otherTbl.getTblMapping().getMappedClass();
        if(Q.equals(thisClass, otherClass, false) || (thisClass==null ^ otherClass==null)) {
            if(thisClass==null) {
                thisClass = otherClass;
            }
            result.getTblMapping().setMappedClass(thisClass);
        }

        // set the Tbl mapping - properties
        for(TblColumn projectedColumn : projection) {
            Pair<String, Double> colMapping = null;

            if(getColumns().contains(projectedColumn)) {
                colMapping = getTblMapping().getMappedProperty(projectedColumn.getColumnIndex());
            } else {
                colMapping = otherTbl.getTblMapping().getMappedProperty(projectedColumn.getColumnIndex());
            }
            if(colMapping!=null) {
                result.getTblMapping().setMappedProperty(inputColumnToOutputColumn.get(projectedColumn).getColumnIndex(), colMapping);
            }
        }

        // create the join
        for(TblRow r : getTblRowArrayList()) {

            // find all rows statisfying the join condition
            Collection<TblRow> matchingRows = null;
            for(Pair<TblColumn, TblColumn> p : joinOn) {
                Object leftValue = r.get(p.getFirst().getColumnIndex());

                Collection<TblRow> otherRows = index.get(p.getSecond()).get(leftValue);

                if(otherRows==null) {
                    matchingRows = null;
                    break;
                }

                if(matchingRows==null) {
                    matchingRows = otherRows;
                } else {
                    matchingRows = Q.intersection(matchingRows, otherRows);
                }
            }

            // iterate over the matching rows
            if(matchingRows!=null && matchingRows.size()>0) {
                for(TblRow r2 : matchingRows) {

                    // create a result row
                    TblRow out = new TblRow(result.getTblRowArrayList().size(), result);
                    Object[] values = new Object[inputColumnToOutputColumn.size()];
                    out.set(values);
                    result.addRow(out);

                    // copy all values from the left Tbl
                    for(TblColumn c : getColumns()) {
                        TblColumn c2 = inputColumnToOutputColumn.get(c);
                        if(c2!=null) {
                            values[c2.getColumnIndex()] = r.get(c.getColumnIndex());
                        }
                    }

                    // copy all values from the right Tbl
                    for(TblColumn c : otherTbl.getColumns()) {
                        TblColumn c2 = inputColumnToOutputColumn.get(c);
                        if(c2!=null) {
                            values[c2.getColumnIndex()] = r2.get(c.getColumnIndex());
                        }
                    }

                    // set the Tbl mapping - instances
                    Pair<String, Double> thisRowMapping = getTblMapping().getMappedInstance(r.getRowNumber());
                    Pair<String, Double> otherRowMapping = otherTbl.getTblMapping().getMappedInstance(r2.getRowNumber());
                    if(Q.equals(thisRowMapping, otherRowMapping, false) || (thisRowMapping==null ^ otherRowMapping==null)) {
                        if(thisRowMapping==null) {
                            thisRowMapping = otherClass;
                        }
                        result.getTblMapping().setMappedInstance(out.getRowNumber(), thisRowMapping);
                    }

                }

            }
        }

        return result;
    }

    public Tbl copySchema() {
        Tbl result = new Tbl();

        // copy the Tbl schema
        for (int i = 0; i < getColumns().size(); i++) {
            TblColumn c = getTblSchema().get(i);

            result.addColumn(c.copy(result, i));
        }

        // copy path and key index
        result.setSubjectColumnIndex(getSubjectColumnIndex());
        result.setPath(getPath());

        // copy functional dependencies
        for (Set<TblColumn> det : getTblSchema().getFunctionalDependencies().keySet()) {
            Set<TblColumn> dep = getTblSchema().getFunctionalDependencies().get(det);
            Set<TblColumn> newDet = new HashSet<>(det.size());

            for (TblColumn c : det) {
                newDet.add(result.getTblSchema().get(c.getColumnIndex()));
            }

            Set<TblColumn> newDep = new HashSet<>(dep.size());
            for (TblColumn c : dep) {
                newDep.add(result.getTblSchema().get(c.getColumnIndex()));
            }

            result.getTblSchema().getFunctionalDependencies().put(newDet, newDep);
        }

        // copy candidate keys
        for (Set<TblColumn> key : getTblSchema().getCandidateKeys()) {
            result.getTblSchema().getCandidateKeys().add(key);
        }

        return result;
    }

    public static enum ConflictHandling {
        KeepFirst, KeepBoth, ReplaceNULLs, CreateList, CreateSet, ReturnConflicts
    }

    public Collection<Pair<TblRow, TblRow>> deduplicate(Collection<TblColumn> key) {
        return deduplicate(key, ConflictHandling.KeepFirst);
    }

    public Collection<Pair<TblRow, TblRow>> deduplicate(Collection<TblColumn> key, ConflictHandling conflictHandling) {
        return deduplicate(key, conflictHandling, true);
    }

    public Collection<Pair<TblRow, TblRow>> deduplicate(Collection<TblColumn> key, ConflictHandling conflictHandling, boolean reorganiseRowNumbers) {

        Collection<Pair<TblRow, TblRow>> duplicates = new LinkedList<>();

        HashMap<List<Object>, TblRow> seenKeyValues = new HashMap<>();

        ArrayList<TblRow> deduplicatedRows = new ArrayList<>(getTblRowArrayList().size());

        Iterator<TblRow> rowIt = getTblRowArrayList().iterator();

        while (rowIt.hasNext()) {
            TblRow r = rowIt.next();

            ArrayList<Object> keyValues = new ArrayList<>(key.size());
            for (TblColumn c : key) {
                keyValues.add(r.get(c.getColumnIndex()));
            }

            boolean keepRow = true;

            if (seenKeyValues.containsKey(keyValues)) {

                TblRow existing = seenKeyValues.get(keyValues);

                if(conflictHandling != ConflictHandling.ReturnConflicts) {
                    duplicates.add(new Pair<>(existing, r));
                }

                if (conflictHandling != ConflictHandling.KeepFirst) {

                    boolean equal = true;
                    boolean conflictingNullsOnly = true;
                    List<Integer> nullIndices = new LinkedList<>();
                    for (TblColumn c : Q.without(getColumns(), key)) {
                        Object existingValue = existing.get(c.getColumnIndex());
                        Object duplicateValue = r.get(c.getColumnIndex());

                        if(Q.equals(existingValue, duplicateValue, true)) {				// both values equal or both NULL
                            // equal values
                        } else if (existingValue == null && duplicateValue != null
                                || existingValue != null && duplicateValue == null) {	// one value NULL
                            equal = false;
                            nullIndices.add(c.getColumnIndex());
                        } else {														// different values
                            equal = false;
                            conflictingNullsOnly = false;
                        }
                    }

                    if (!equal) {
                        // the records are not equal
                        if (conflictHandling == ConflictHandling.KeepBoth
                                || conflictHandling == ConflictHandling.ReplaceNULLs && !conflictingNullsOnly) {
                            // if handling is set to keep both we don't merge
                            // if handling is set to replace nulls, but there is
                            // a conflict between non-null values, we don't
                            // merge
                            // continue;
                            keepRow = true;
                        } else if(conflictHandling == ConflictHandling.ReturnConflicts) {
                            duplicates.add(new Pair<>(existing, r));
                            keepRow = true;
                        } else if(conflictHandling == ConflictHandling.CreateList || conflictHandling == ConflictHandling.CreateSet) {
                            // if handling is set to create list or create set, we merge all values and  assign them to the first record

                            for (TblColumn c : Q.without(getColumns(), key)) {

                                Object existingValue = existing.get(c.getColumnIndex());
                                Object conflictingValue = r.get(c.getColumnIndex());
                                Collection<Object> values = null;
                                if(conflictHandling==ConflictHandling.CreateSet) {
                                    values = new HashSet<>();
                                } else {
                                    values = new LinkedList<>();
                                }

                                if(existingValue!=null) {
                                    if(existingValue.getClass().isArray()) {
                                        values.addAll(Q.toList((Object[])existingValue));
                                    } else {
                                        values.add(existingValue);
                                    }
                                }

                                if(conflictingValue!=null) {
                                    if(conflictingValue.getClass().isArray()) {
                                        values.addAll(Q.toList((Object[])conflictingValue));
                                    } else {
                                        values.add(conflictingValue);
                                    }
                                }

                                if(values.size()<=1) {
                                    // if the result has only one element, don't treat it as multi-valued
                                    existing.set(c.getColumnIndex(), Q.firstOrDefault(values));
                                } else {
                                    existing.set(c.getColumnIndex(), values.toArray());
                                }
                            }

                            keepRow = false;
                        } else {
                            // if handling is set to replace nulls, and there
                            // are only conflicts between values and nulls, we
                            // set the values in the existing record and remove
                            // the second record
                            for (Integer idx : nullIndices) {
                                if (existing.get(idx) == null) {
                                    existing.set(idx, r.get(idx));
                                }
                            }

                            keepRow = false;
                        }
                    } else {
                        keepRow = false;
                    }
                } else {
                    keepRow = false;
                }

                if(!keepRow) {
                    // remove the duplicate row
                    // rowIt.remove();

                    // and add the table name of the duplicate row to the existing
                    // row
                    existing.addProvenanceForRow(r);
                }
            } else {
                // if not, add the current key values to the list of seen values
                seenKeyValues.put(keyValues, r);

                // add the row itself as provenance information (so we have all
                // source information if later rows are merged with this one)
                // r.addProvenanceForRow(r);
            }

            if(keepRow) {
                // add the row to the output
                deduplicatedRows.add(r);
            }
        }

        // re-create the array list
        // setRows(new ArrayList<>(linkedRows));
        setTblRowArrayList(deduplicatedRows);
        tblRowArrayList.trimToSize();

        if(reorganiseRowNumbers) {
            reorganiseRowNumbers();
        }

        return duplicates;
    }

}
