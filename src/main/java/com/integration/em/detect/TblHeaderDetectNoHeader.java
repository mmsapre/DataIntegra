package com.integration.em.detect;

public class TblHeaderDetectNoHeader implements TblHeaderDetect{
    @Override
    public int[] detectTableHeader(String[][] attributeValues, int[] skipRows) {
        return null;
    }
}
