package com.integration.em.detect;

public class TblHeaderDetectFirstRow implements TblHeaderDetect{
    @Override
    public int[] detectTableHeader(String[][] attributeValues, int[] skipRows) {
        if(skipRows == null){
            int[] result = {0};
            return result;
        }else{
            int[] result = {skipRows.length};
            return result;
        }
    }
}
