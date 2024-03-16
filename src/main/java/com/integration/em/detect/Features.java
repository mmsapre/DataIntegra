package com.integration.em.detect;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.AnnotationPipeline;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.time.TimeAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
public class Features {

    DecimalFormat df = new DecimalFormat("#.##");
    private MaxentTagger maxentTagger;
    private double FractionofCellswithNumnericContent = -1;
    private double AverageNumberofDataTokensinEachCell = -1;
    private double AverageNumberofSpecialCharactersinEachCell = -1;
    private double AverageNumberofPunctuationsinEachCell = -1;
    private boolean IsAlphanumeric = false;
    private String CellContentPattern = null;

    private double PercentageofNumericCharacters = -1;
    private double PercentageofAlphabeticCharacters = -1;
    private double PercentageofSpecialCharacters = -1;
    private double PercentageofPunctuationCharacters = -1;

    private String POSPatternofColumn = null;
    private String POSPatternofHeaderCell = null;

    private int NumberofDistinctValuesinColumn = -1;

    private boolean HasHeaderCell = false;
    private boolean ContainSpecialCharactersinHeaderCell = false;
    private boolean ContainPunctuationCharactersinHeaderCell = false;

    // features added after first successful run of model
    private boolean IsDateorTime = false;
    private int AverageCharacterLenghth = -1;
    private boolean IsBooleanValue = false;

    OthOps otherOperations = new OthOps();

    public Features(MaxentTagger maxentTagger) {
        super();
        this.maxentTagger = maxentTagger;
    }

    public double getFractionofCellswithNumnericContent() {
        return FractionofCellswithNumnericContent;
    }

    public void setFractionofCellswithNumnericContent(
            double fractionofCellswithNumnericContent) {
        FractionofCellswithNumnericContent = fractionofCellswithNumnericContent;
    }

    public double getAverageNumberofDataTokensinEachCell() {
        return AverageNumberofDataTokensinEachCell;
    }

    public void setAverageNumberofDataTokensinEachCell(
            double averageNumberofDataTokensinEachCell) {
        AverageNumberofDataTokensinEachCell = averageNumberofDataTokensinEachCell;
    }

    public double getAverageNumberofSpecialCharactersinEachCell() {
        return AverageNumberofSpecialCharactersinEachCell;
    }

    public void setAverageNumberofSpecialCharactersinEachCell(
            double averageNumberofSpecialCharactersinEachCell) {
        AverageNumberofSpecialCharactersinEachCell = averageNumberofSpecialCharactersinEachCell;
    }

    public double getAverageNumberofPunctuationsinEachCell() {
        return AverageNumberofPunctuationsinEachCell;
    }

    public void setAverageNumberofPunctuationsinEachCell(
            double averageNumberofPunctuationsinEachCell) {
        AverageNumberofPunctuationsinEachCell = averageNumberofPunctuationsinEachCell;
    }

    public boolean isIsAlphanumeric() {
        return IsAlphanumeric;
    }

    public void setIsAlphanumeric(boolean isAlphanumeric) {
        IsAlphanumeric = isAlphanumeric;
    }

    public String getCellContentPattern() {
        return CellContentPattern;
    }

    public void setCellContentPattern(String cellContentPattern) {
        CellContentPattern = cellContentPattern;
    }

    public double getPercentageofNumericCharacters() {
        return PercentageofNumericCharacters;
    }

    public void setPercentageofNumericCharacters(
            double percentageofNumericCharacters) {
        PercentageofNumericCharacters = percentageofNumericCharacters;
    }

    public double getPercentageofAlphabeticCharacters() {
        return PercentageofAlphabeticCharacters;
    }

    public void setPercentageofAlphabeticCharacters(
            double percentageofAlphabeticCharacters) {
        PercentageofAlphabeticCharacters = percentageofAlphabeticCharacters;
    }

    public double getPercentageofSpecialCharacters() {
        return PercentageofSpecialCharacters;
    }

    public void setPercentageofSpecialCharacters(
            double percentageofSpecialCharacters) {
        PercentageofSpecialCharacters = percentageofSpecialCharacters;
    }

    public double getPercentageofPunctuationCharacters() {
        return PercentageofPunctuationCharacters;
    }

    public void setPercentageofPunctuationCharacters(
            double percentageofPunctuationCharacters) {
        PercentageofPunctuationCharacters = percentageofPunctuationCharacters;
    }

    public boolean isHasHeaderCell() {
        return HasHeaderCell;
    }

    public void setHasHeaderCell(boolean hasHeader) {
        HasHeaderCell = hasHeader;
    }

    public boolean isContainSpecialCharactersinHeaderCell() {
        return ContainSpecialCharactersinHeaderCell;
    }

    public void setContainSpecialCharactersinHeaderCell(
            boolean containSpecialCharactersinHeaderRow) {
        ContainSpecialCharactersinHeaderCell = containSpecialCharactersinHeaderRow;
    }

    public boolean isContainPunctuationCharactersinHeaderCell() {
        return ContainPunctuationCharactersinHeaderCell;
    }

    public void setContainPunctuationCharactersinHeaderCell(
            boolean containPunctuationCharactersinHeaderRow) {
        ContainPunctuationCharactersinHeaderCell = containPunctuationCharactersinHeaderRow;
    }

    public String getPOSPatternofColumn() {
        return POSPatternofColumn;
    }

    public void setPOSPatternofColumn(String pOSPatternofCell) {
        POSPatternofColumn = pOSPatternofCell;
    }

    public String getPOSPatternofHeaderCell() {
        return POSPatternofHeaderCell;
    }

    public void setPOSPatternofHeaderCell(String pOSPatternofHeaderCell) {
        POSPatternofHeaderCell = pOSPatternofHeaderCell;
    }

    public int getNumberofDistinctValuesinColumn() {
        return NumberofDistinctValuesinColumn;
    }

    public void setNumberofDistinctValuesinColumn(
            int numberofDistinctValuesinColumn) {
        NumberofDistinctValuesinColumn = numberofDistinctValuesinColumn;
    }

    public int getAverageCharacterLenghth() {
        return AverageCharacterLenghth;
    }

    public void setAverageCharacterLenghth(int characterLenghthuptoFive) {
        this.AverageCharacterLenghth = characterLenghthuptoFive;
    }

    public boolean isIsDateorTime() {
        return IsDateorTime;
    }

    public void setIsDateorTime(boolean isDateorTime) {
        IsDateorTime = isDateorTime;
    }

    public boolean isIsBooleanValue() {
        return IsBooleanValue;
    }

    public void setIsBooleanValue(boolean isBooleanValue) {
        IsBooleanValue = isBooleanValue;
    }

    public void createFeatures(String[] column, AnnotationPipeline pipeline) {

        int rowCounter = 0;
        String headerCelltemp = "";
        Map<String, Integer> ccpList = new TreeMap<String, Integer>();
        int length = 0;
        int resultSUTParser = 0;
        int resultBooleanValue = 0;

        // Loop once through one column
        for (String cell : column) {

            if (cell == null)
                continue;
            else {

                if (rowCounter < 2) {
                    headerCelltemp = prepareHasHeaderCell(cell, headerCelltemp);
                }
                ccpList = prepareCellContentPattern(cell, ccpList);
                length = prepareAvgCharLength(cell, length);
                resultSUTParser = prepareSUTParser(cell, pipeline,
                        resultSUTParser);
                resultBooleanValue = prepareBooleanValue(cell,
                        resultBooleanValue);
                rowCounter++;
            }
        }

        validateHasHeaderCell(headerCelltemp);
        validateCellContentPattern(ccpList);
        setAverageCharacterLenghth(length / column.length);
        validateSUTParser(resultSUTParser, column.length);
        validateBooleanValue(resultBooleanValue, column.length);

        if (isHasHeaderCell() && column[0] != null) {
            containPunctuationCharactersinHeaderCell(column[0]);
        }
        if (column[0] != null)
            posPatternofHeaderCell(column[0]);

        // check for whole content
        String content = otherOperations.getColumnContentWithoutSpaces(column);
        validatePercentageofAlphabeticCharacters(content);
        validatePercentageofPunctuationCharacters(content);

    }

    private void validateHasHeaderCell(String headerCelltemp) {
        if (headerCelltemp.trim().split("\\s").length >= 2) {
            if (headerCelltemp.split("\\s")[0]
                    .equals(headerCelltemp.split("\\s")[1]))
                setHasHeaderCell(false);
            else
                setHasHeaderCell(true);
        } else
            setHasHeaderCell(false);
    }

    private String prepareHasHeaderCell(String cell, String headerCelltemp) {
        String textPattern = null;

        if (!cell.trim().isEmpty() && !cell.trim().equals("-")
                && !cell.trim().equals("--") && !cell.trim().equals("---")
                && !cell.trim().equals("n/a") && !cell.trim().equals("N/A")
                && !cell.trim().equals("(n/a)")
                && !cell.trim().equals("Unknown")
                && !cell.trim().equals("unknown") && !cell.trim().equals("?")
                && !cell.trim().equals("??") && !cell.trim().equals(".")) {
            textPattern = cell //
                    .replace("\\s", "").replaceAll("[a-zA-Z]+", "a") // alphabetical
                    .replaceAll("[0-9]+", "d")// digits
                    // http://www.enchantedlearning.com/grammar/punctuation/
                    .replaceAll("[^a-zA-z\\d\\s.!;():?,\\-'\"]+", "s")// special

                    .replaceAll("[\\s.!;():?,\\-'\"]+", "p"); // punctuation
            headerCelltemp = headerCelltemp + textPattern + " ";
        }
        return headerCelltemp;
    }

    private void validatePercentageofAlphabeticCharacters(String content) {
        if (content.length() == 0)
            setPercentageofAlphabeticCharacters(-1);
        String alphabeticContent = content.replaceAll("[^a-zA-Z]", "");
        if ((double) alphabeticContent.length() != 0) {
            double result = ((double) alphabeticContent.length())
                    / content.length();
            if (result != 0.0)
//				setPercentageofAlphabeticCharacters(Double.valueOf(df.format(result))); // why?
                setPercentageofAlphabeticCharacters(result);
            else
                setPercentageofAlphabeticCharacters(-1);
        } else
            setPercentageofAlphabeticCharacters(-1);
    }

    private void validatePercentageofPunctuationCharacters(String content) {
        if (content.length() == 0)
            setPercentageofPunctuationCharacters(-1);
        String punctuationContent = content.replaceAll("[^\\s.!;():?,\\-'\"]+",
                "");
        if ((double) punctuationContent.length() != 0) {
            double result = ((double) punctuationContent.length())
                    / content.length();
            if (result != 0)
                setPercentageofPunctuationCharacters(result);
            else
                setPercentageofPunctuationCharacters(-1);
        } else
            setPercentageofPunctuationCharacters(-1);
    }

    private Map<String, Integer> prepareCellContentPattern(String cell,
                                                           Map<String, Integer> ccpList) {
        String textPattern = null;
        if ((!cell.trim().isEmpty()) && (!cell.trim().equals("-")
                && !cell.trim().equals("--") && !cell.trim().equals("---")
                && !cell.trim().equals("n/a") && !cell.trim().equals("N/A")
                && !cell.trim().equals("(n/a)")
                && !cell.trim().equals("Unknown")
                && !cell.trim().equals("unknown") && !cell.trim().equals("?")
                && !cell.trim().equals("??") && !cell.trim().equals(".")
                && !cell.trim().equals("null") && !cell.trim().equals("NULL")
                && !cell.trim().equals("Null"))) {
            textPattern = cell //
                    .replace("\\s", "").replaceAll("[a-zA-Z]+", "a") // alphabetical
                    .replaceAll("[0-9]+", "d")// digits
                    .replaceAll("[^a-zA-z\\d\\s.!;():?,\\-'\"]+", "s")// special

                    .replaceAll("[\\s.!;():?,\\-'\"]+", "p"); // punctuation

            if (ccpList.containsKey(textPattern))
                ccpList.put(textPattern, ccpList.get(textPattern) + 1);
            else
                ccpList.put(textPattern, 1);
        }
        return ccpList;
    }

    private void validateCellContentPattern(Map<String, Integer> ccpList) {
        if (!ccpList.isEmpty()) {
            setCellContentPattern(
                    (otherOperations.entriesSortedByValues(ccpList)).last()
                            .getKey());
        } else
            setCellContentPattern(null);
        ccpList.clear();
    }

    public void containPunctuationCharactersinHeaderCell(String cell) {
        if ((!cell.trim().isEmpty()) && (!cell.trim().equals("-")
                && !cell.trim().equals("--") && !cell.trim().equals("---")
                && !cell.trim().equals("n/a") && !cell.trim().equals("N/A")
                && !cell.trim().equals("(n/a)")
                && !cell.trim().equals("Unknown")
                && !cell.trim().equals("unknown") && !cell.trim().equals("?")
                && !cell.trim().equals("??") && !cell.trim().equals(".")
                && !cell.trim().equals("null") && !cell.trim().equals("NULL")
                && !cell.trim().equals("Null"))) {
            String temp = cell.replaceAll("[^\\s.!;():?,\\-'\"]+", "").trim();
            if (temp.length() < 1)
                setContainPunctuationCharactersinHeaderCell(false);
            else
                setContainPunctuationCharactersinHeaderCell(true);
        } else
            setContainPunctuationCharactersinHeaderCell(false);
    }

    public void posPatternofHeaderCell(String cell) {
        Map<String, Integer> posPatternHD = new TreeMap<String, Integer>();

        if ((!cell.trim().isEmpty()) && (!cell.trim().equals("-")
                && !cell.trim().equals("--") && !cell.trim().equals("---")
                && !cell.trim().equals("n/a") && !cell.trim().equals("N/A")
                && !cell.trim().equals("(n/a)")
                && !cell.trim().equals("Unknown")
                && !cell.trim().equals("unknown") && !cell.trim().equals("?")
                && !cell.trim().equals("??") && !cell.trim().equals(".")
                && !cell.trim().equals("null") && !cell.trim().equals("NULL")
                && !cell.trim().equals("Null"))) {
            // The tagged string
            String tagged = maxentTagger.tagString(cell);

            String[] temp = tagged.split("\\s");
            String POSPattern = "";
            for (String pattern : temp) {
                POSPattern += pattern.substring(pattern.indexOf("_") + 1) + "-";
            }

            String POSPatternofCell = POSPattern.trim()
                    .substring(0, POSPattern.length() - 1)
                    .replaceAll("--", "-");


            if (posPatternHD.containsKey(POSPatternofCell))
                posPatternHD.put(POSPatternofCell,
                        posPatternHD.get(POSPatternofCell) + 1);
            else
                posPatternHD.put(POSPatternofCell, 1);

            // posPatternHD = otherOperations.sortByValue(posPatternHD);
            setPOSPatternofHeaderCell(
                    (otherOperations.entriesSortedByValues(posPatternHD)).last()
                            .getKey());
        } else
            setPOSPatternofHeaderCell(null);

        posPatternHD.clear();
    }

    private int prepareAvgCharLength(String cell, int length) {
        if ((!cell.trim().isEmpty()) && (!cell.trim().equals("-")
                && !cell.trim().equals("--") && !cell.trim().equals("---")
                && !cell.trim().equals("n/a") && !cell.trim().equals("N/A")
                && !cell.trim().equals("(n/a)")
                && !cell.trim().equals("Unknown")
                && !cell.trim().equals("unknown") && !cell.trim().equals("?")
                && !cell.trim().equals("??") && !cell.trim().equals(".")
                && !cell.trim().equals("null") && !cell.trim().equals("NULL")
                && !cell.trim().equals("Null")))
            length = length + cell.trim().length();
        return length;
    }

    private int prepareSUTParser(String cell, AnnotationPipeline pipeline,
                                 int result) {
        if ((!cell.trim().isEmpty()) && (!cell.trim().equals("-")
                && !cell.trim().equals("--") && !cell.trim().equals("---")
                && !cell.trim().equals("n/a") && !cell.trim().equals("N/A")
                && !cell.trim().equals("(n/a)")
                && !cell.trim().equals("Unknown")
                && !cell.trim().equals("unknown") && !cell.trim().equals("?")
                && !cell.trim().equals("??") && !cell.trim().equals(".")
                && !cell.trim().equals("null") && !cell.trim().equals("NULL")
                && !cell.trim().equals("Null"))) {
            Annotation annotation = new Annotation(cell);
            annotation.set(CoreAnnotations.DocDateAnnotation.class,
                    "2013-07-14");
            pipeline.annotate(annotation);

            List<CoreMap> timexAnnsAll = annotation
                    .get(TimeAnnotations.TimexAnnotations.class);
            if (timexAnnsAll != null)
                if (!timexAnnsAll.isEmpty())
                    result++;
        }
        return result;
    }

    private void validateSUTParser(int result, int columnLength) {
        if (result > columnLength / 2)
            setIsDateorTime(true);
        else
            setIsDateorTime(false);
    }

    private int prepareBooleanValue(String cell, int resultBooleanValue) {
        if ((!cell.trim().isEmpty()) && (!cell.trim().equals("-")
                && !cell.trim().equals("--") && !cell.trim().equals("---")
                && !cell.trim().equals("n/a") && !cell.trim().equals("N/A")
                && !cell.trim().equals("(n/a)")
                && !cell.trim().equals("Unknown")
                && !cell.trim().equals("unknown") && !cell.trim().equals("?")
                && !cell.trim().equals("??") && !cell.trim().equals(".")
                && !cell.trim().equals("null") && !cell.trim().equals("NULL")
                && !cell.trim().equals("Null"))) {
            if (cell.trim().equals("yes") || cell.trim().equals("Yes")
                    || cell.trim().equals("YES") || cell.trim().equals("no")
                    || cell.trim().equals("No") || cell.trim().equals("NO")
                    || cell.trim().equals("1") || cell.trim().equals("0")
                    || cell.trim().equals("true") || cell.trim().equals("True")
                    || cell.trim().equals("TRUE") || cell.trim().equals("false")
                    || cell.trim().equals("False")
                    || cell.trim().equals("FALSE"))
                resultBooleanValue++;
        }
        return resultBooleanValue;
    }

    private void validateBooleanValue(int result, int columnLength) {
        if (result > columnLength / 2)
            setIsBooleanValue(true);
        else
            setIsBooleanValue(false);
    }
}
