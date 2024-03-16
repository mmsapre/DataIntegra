package com.integration.em.detect;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class TblHeaderDetectContent implements TblHeaderDetect{
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
        int flag = 0;
        String[] firstRow = myRowMap.get(0);
        String[] secondRow = myRowMap.get(1);

        for (int i = 0; i < firstRow.length; i++) {

            if(firstRow[i].length() <= 10){
                if(extractPatternFromCell(firstRow[i]).equals(extractPatternFromCell(secondRow[i]))) {
                    flag ++;
                }
            }
        }

        if(flag < myRowMap.get(1).length - 1){
            flag = 0;
            Pattern emailpattern = Pattern.compile("^.+@.+\\..+$");
            Matcher emailMatcher;
            for(int i = 1; i < myRowMap.size() - 1; i++){
                String[] currentRow = myRowMap.get(i);
                String[] nextRow = myRowMap.get(i + 1);
                for (int j = 0; j < myRowMap.get(1).length; j++) {
                    emailMatcher = emailpattern.matcher(currentRow[j]);
                    if(emailMatcher.matches()){
                        break;
                    }
                    else if(!extractPatternFromCell(currentRow[j]).equals(extractPatternFromCell(nextRow[j]))) {
                        flag++;
                    }
                }
            }

            if(flag > myRowMap.size()*(2)){
                return null;
            }
            else{
                int[] result = new int[]{0};
                return result;
            }
        }
        else{
            return null;
        }

    }

    public String extractPatternFromCell(String cell) {
        String cellPattern = cell //
                .replace("\\s", "")
                .replaceAll("[a-zA-Z]+", "a")
                .replaceAll("[0-9]+", "d")
                .replaceAll("[^ad\\s.!;():?,\\-'\"]+", "s")
                .replaceAll("[\\s.!;():?,\\-'\"]+", "p");

        return cellPattern;
    }
}
