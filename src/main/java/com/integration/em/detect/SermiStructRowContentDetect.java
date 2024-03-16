package com.integration.em.detect;

import java.util.ArrayList;
import java.util.List;

public class SermiStructRowContentDetect implements RowContentDetect{

    private static final List<String> totalRowIndicators = new ArrayList<String>() {

        private static final long serialVersionUID = 1L;

        {
            add("total");
            add("sum");
        }
    };
    @Override
    public int[] detectEmptyHeaderRows(String[][] attributeValues, boolean columBased) {
        int emptyRows = -1;
        if (columBased) {
            for (int rowIdx = 0; rowIdx < attributeValues[0].length; rowIdx++) {
                boolean empty = true;
                for (int columnIdx = 0; columnIdx < attributeValues.length; columnIdx++) {
                    String value = attributeValues[columnIdx][rowIdx];
                    if (value!=null && !value.equals("")) {
                        empty = false;
                        break;
                    }
                }
                if (empty) {
                    emptyRows = rowIdx;
                } else {
                    break;
                }
            }
        } else {
            for (int rowIdx = 0; rowIdx < attributeValues.length; rowIdx++) {
                String[] rowData = attributeValues[rowIdx];
                boolean empty = true;
                for (String value : rowData) {
                    if (value!=null && !value.equals("")) {
                        empty = false;
                        break;
                    }
                }
                if (empty) {
                    emptyRows = rowIdx;
                } else {
                    break;
                }
            }

        }
        if (emptyRows > -1) {
            int[] result = new int[emptyRows + 1];
            for (int emptyRow = 0; emptyRow < result.length; emptyRow++) {
                result[emptyRow] = emptyRow;
            }
            return result;
        }
        return null;
    }

    @Override
    public int[] detectSumRow(String[][] attributeValues) {
        String value = attributeValues[attributeValues.length - 1][0];
        if(value!=null) {
            value = value.toLowerCase();
            if (totalRowIndicators.contains(value)) {
                int[] result = { attributeValues.length - 1 };
                return result;
            }
        }
        return null;
    }
}
