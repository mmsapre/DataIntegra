package com.integration.em.detect;

public interface RowContentDetect {

    int[] detectEmptyHeaderRows(String[][] attributeValues, boolean columBased);

    int[] detectSumRow(String[][] attributeValues);
}
