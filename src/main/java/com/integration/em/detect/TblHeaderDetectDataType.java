package com.integration.em.detect;
import java.util.HashMap;
import java.util.Map;
public class TblHeaderDetectDataType implements TblHeaderDetect{
    @Override
    public int[] detectTableHeader(String[][] attributeValues, int[] skipRows) {
        int analysedRows = 5;
        if(skipRows != null){
            analysedRows = analysedRows + skipRows.length - 1;
        }
        Map<Integer, String[]> myRowMap = new HashMap<Integer, String[]>(analysedRows);
        for (int i=0; i<analysedRows; i++)
        {
            myRowMap.put(i, (String[]) attributeValues[i]);
        }

        boolean[] isString = new boolean[myRowMap.size()];
        for (int i = 0;i < myRowMap.size(); i++) {
            isString[i] = isStringOnly(myRowMap.get(i));
        }

        for(int i = 1; i < isString.length; i++){
            if(isString[0] && isString[i]){
                return null;
            }
        }
        int[] result = new int[]{0};
        return result;
    }

    public boolean isStringOnly(String[] columnORrow)
    {
        int alphaCount = 0, anyCount = 0;
        if (columnORrow.length > 0) {

            for(String str : columnORrow){
                for (char c : str.toCharArray()) {
                    if(Character.isAlphabetic(c) || Character.isSpaceChar(c)) {
                        alphaCount++;
                    }
                    else{
                        anyCount++;
                    }
                }
            }
        }

        if(alphaCount > 0 && anyCount == 0)
            return true;
        else
            return false;

    }

}
