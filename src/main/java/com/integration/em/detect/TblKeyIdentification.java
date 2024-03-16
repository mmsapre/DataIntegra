package com.integration.em.detect;

import com.integration.em.datatypes.DataType;
import com.integration.em.tables.Tbl;
import com.integration.em.tables.TblColumn;
import com.integration.em.tables.TblRow;
import com.integration.em.utils.Q;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
public class TblKeyIdentification {


    private static final Pattern prefLabelPattern = Pattern.compile("([^#]*#)?([a-z]{1,9})?prefLabel$");
    private static final Pattern namePattern = Pattern.compile("([^#]*#)?name$");
    private static final Pattern labelPattern = Pattern.compile("([^#]*#)?([a-z]{1,9})?label$");
    private static final Pattern titlePattern = Pattern.compile("([^#]*#)?([a-z]{1,9})?title$");
    private static final Pattern labelPattern2 = Pattern.compile("([^#]*#)?.*Label$");
    private static final Pattern namePattern2 = Pattern.compile("([^#]*#)?.*Name$");
    private static final Pattern titlePattern2 = Pattern.compile("([^#]*#)?.*Title$");
    private static final Pattern alternateNamePattern = Pattern.compile("([^#]*#)?([a-z]{1,9})?alternateName$");

    private double keyUniquenessThreshold;

    public double getKeyUniquenessThreshold() {
        return keyUniquenessThreshold;
    }

    public void setKeyUniquenessThreshold(double keyUniquenessThreshold) {
        this.keyUniquenessThreshold = keyUniquenessThreshold;
    }

    public void identifyKeys(Tbl Tbl) {
        TblColumn key = null;
        int keyColumnIndex = -1;
        List<Double> columnUniqueness = new ArrayList<>(Tbl.getColumns().size());
        List<Double> columnValueLength = new ArrayList<>(Tbl.getColumns().size());

        for (int i = 0; i < Tbl.getTblSchema().getSize(); i++) {

            int nullCount = 0;
            int numRows = 0;
            List<Integer> valueLength = new ArrayList<>(Tbl.getSize());
            HashSet<Object> uniqueValues = new HashSet<>();

            for (TblRow r : Tbl.getTblRowArrayList()) {
                Object value = r.get(i);
                if (value != null) {
                    uniqueValues.add(value);
                    // valueCount++;
                    valueLength.add(value.toString().length());
                } else {
                    nullCount++;
                }
                numRows++;
            }

            double uniqueness = (double) uniqueValues.size() / (double) numRows;
            double nullness = (double) nullCount / (double) numRows;

            columnUniqueness.add(uniqueness - nullness);
            columnValueLength.add(Q.average(valueLength));

            TblColumn c = Tbl.getTblSchema().get(i);
            log.info(String.format("[%d]%s (%s) Uniqueness=%.4f; Nullness=%.4f; Combined=%.4f; Length=%.4f", i,
                    c.getHeader(), c.getDataType(), uniqueness, nullness,
                    columnUniqueness.get(columnUniqueness.size() - 1),
                    columnValueLength.get(columnValueLength.size() - 1)));
        }

        for (int i = Tbl.getColumns().size() - 1; i >= 0; i--) {
            TblColumn column = Tbl.getTblSchema().get(i);

            if (column.getDataType() != DataType.string) {
                continue;
            }
            if (prefLabelPattern.matcher(column.getHeader()).matches()) {
                key = column;
                break;
            }
            if (namePattern.matcher(column.getHeader()).matches()) {
                key = column;
                break;
            }
            if (labelPattern.matcher(column.getHeader()).matches()) {
                key = column;
            }

            if (titlePattern.matcher(column.getHeader()).matches()) {
                key = column;
            }
            if (labelPattern2.matcher(column.getHeader()).matches()) {
                key = column;
            }

            if (namePattern2.matcher(column.getHeader()).matches()) {
                key = column;
            }

            if (titlePattern2.matcher(column.getHeader()).matches()) {
                key = column;
            }
            if (alternateNamePattern.matcher(column.getHeader()).matches()) {
                key = column;
            }

        }

        if (key != null) {
            keyColumnIndex = Tbl.getTblSchema().indexOf(key);

            if (columnUniqueness.get(keyColumnIndex) >= getKeyUniquenessThreshold()
                    && columnValueLength.get(keyColumnIndex) > 3.5 && columnValueLength.get(keyColumnIndex) <= 200) {

                Tbl.setSubjectColumnIndex(keyColumnIndex);

                log.info(
                        String.format("RegEx Header Match: '%s'", Tbl.getTblSchema().get(keyColumnIndex).getHeader()));

                return;
            }

            key = null;

            log.info(String.format("RegEx Header Match: '%s' - insufficient",
                    Tbl.getTblSchema().get(keyColumnIndex).getHeader()));
        }

        if (columnUniqueness.isEmpty()) {
            log.info("no columns");
            return;
        }
        double maxCount = -1;
        int maxColumn = -1;

        for (int i = 0; i < columnUniqueness.size(); i++) {
            if (columnUniqueness.get(i) > maxCount && Tbl.getTblSchema().get(i).getDataType() == DataType.string
                    && columnValueLength.get(i) > 3.5 && columnValueLength.get(i) <= 200) {
                maxCount = (Double) columnUniqueness.get(i);
                maxColumn = i;
            }
        }

        if (key == null) {
            if (maxColumn == -1) {
                log.info("no columns that match criteria (data type, min length, max length)");
                return;
            }
            key = Tbl.getTblSchema().get(maxColumn);
        }
        keyColumnIndex = Tbl.getTblSchema().indexOf(key);

        if (columnUniqueness.get(keyColumnIndex) < getKeyUniquenessThreshold()) {

            log.info(String.format("Most unique column: '%s' - insufficient (%.4f)",
                    Tbl.getTblSchema().get(keyColumnIndex).getHeader(), columnUniqueness.get(keyColumnIndex)));

            return;
        }

        log.info(String.format("[TblKeyIdentification] Most unique column: '%s'",
                Tbl.getTblSchema().get(keyColumnIndex).getHeader()));
        Tbl.setSubjectColumnIndex(keyColumnIndex);
    }
}
