package com.integration.em.tables;

import com.integration.em.datatypes.DataType;

import javax.swing.table.TableColumn;
import java.util.ArrayList;
import java.util.Collections;

public class ListFeature implements Feature{
    @Override
    public double calculate(Tbl t) {
        for(TblColumn tc : t.getColumns()) {
            if(tc.getDataType()!= DataType.string) {
                return 0.0;
            }
        }

        ArrayList<String> values = new ArrayList<>();
        ArrayList<String> valuesSorted1 = new ArrayList<>();
        ArrayList<String> valuesSorted2 = new ArrayList<>();

        for(TblRow r : t.getTblRowArrayList()) {
            for(TblColumn tc : t.getColumns()) {
                String value = (String)r.get(tc.getColumnIndex());
                if(value!=null && !value.trim().isEmpty()) {
                    valuesSorted1.add(value);
                    values.add(value);
                }
            }
        }

        for(TblColumn tc : t.getColumns()) {
            for(TblRow r : t.getTblRowArrayList()) {
                String value = (String)r.get(tc.getColumnIndex());
                if(value!=null && !value.trim().isEmpty()) {
                    valuesSorted2.add(value);
                }
            }
        }

        Collections.sort(values);

        boolean isSorted = values.equals(valuesSorted1) || values.equals(valuesSorted2);

        return isSorted ? 1.0 : 0.0;
    }
}
