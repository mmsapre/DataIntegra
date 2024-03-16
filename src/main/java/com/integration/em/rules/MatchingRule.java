package com.integration.em.rules;

import com.integration.em.model.*;
import com.integration.em.processing.Processable;
import com.integration.em.processing.RecordMapper;
import com.integration.em.rules.compare.IComparator;
import com.integration.em.rules.compare.IComparatorLogger;
import com.integration.em.tables.Pair;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import com.integration.em.model.Record;

@Slf4j
public abstract class MatchingRule<RecordType extends Matchable, SchemaElementType extends Matchable>
        implements IComparator<RecordType, SchemaElementType>,
        RecordMapper<Aligner<RecordType, SchemaElementType>, Aligner<RecordType, SchemaElementType>> {

    private double finalThreshold;

    private MixedHashedDataSet<Record, Attribute> comparatorLog;
    private MixedHashedDataSet<Record, Attribute> comparatorLogShort;
    private boolean collectDebugResults = false;
    private HashMap<Attribute, Attribute> resultToComparatorLog;
    private HashMap<String, Attribute> comparatorToResultLog;
    private List<Attribute> headerDebugResults;
    private List<Attribute> headerDebugResultsShort;
    private String filePathDebugResults;
    private int	maxDebugLogSize;
    private BestMatchStandard bestMatchStandard;

    private IComparatorLogger iComparatorLogger;

    public final Attribute MATCHINGRULE = new Attribute("MatchingRule");
    public final Attribute RECORD1IDENTIFIER = new Attribute("Record1Identifier");
    public final Attribute RECORD2IDENTIFIER = new Attribute("Record2Identifier");
    public final Attribute TOTALSIMILARITY = new Attribute("TotalSimilarity");
    public final Attribute ATTRIBUTE_IS_MATCH = new Attribute("IsMatch");

    public MatchingRule(double finalThreshold) {
        this.finalThreshold = finalThreshold;
    }

    public double getFinalThreshold() {
        return finalThreshold;
    }

    public void setFinalThreshold(double finalThreshold) {
        this.finalThreshold = finalThreshold;
    }
    public boolean isDebugReportActive() {
        return collectDebugResults;
    }

    protected boolean continueCollectDebugResults() {
        if(this.maxDebugLogSize == -1 || this.comparatorLog.size() < this.maxDebugLogSize){
            return true;
        }
        else{
            return false;
        }
    }

    public MixedHashedDataSet<Record, Attribute> getComparatorLog() {
        return comparatorLog;
    }

    private void setCollectDebugResults(boolean collectDebugResults) {
        this.collectDebugResults = collectDebugResults;
        if (this.collectDebugResults) {
            initializeMatchingResults();
        }
    }

    public HashMap<Attribute, Attribute> getResultToComparatorLog() {
        return resultToComparatorLog;
    }


    protected void initializeMatchingResults() {
        this.comparatorLog = new MixedHashedDataSet<Record, Attribute>();
        this.comparatorLogShort = new MixedHashedDataSet<Record, Attribute>();
        this.headerDebugResults = new LinkedList<Attribute>();
        this.headerDebugResultsShort = new LinkedList<Attribute>();

        this.comparatorLog.addAttribute(this.MATCHINGRULE);
        this.comparatorLogShort.addAttribute(this.MATCHINGRULE);
        this.headerDebugResults.add(this.MATCHINGRULE);
        this.headerDebugResultsShort.add(this.MATCHINGRULE);

        this.comparatorLog.addAttribute(this.RECORD1IDENTIFIER);
        this.comparatorLogShort.addAttribute(this.RECORD1IDENTIFIER);
        this.headerDebugResults.add(this.RECORD1IDENTIFIER);
        this.headerDebugResultsShort.add(this.RECORD1IDENTIFIER);

        this.comparatorLog.addAttribute(this.RECORD2IDENTIFIER);
        this.comparatorLogShort.addAttribute(this.RECORD2IDENTIFIER);
        this.headerDebugResults.add(this.RECORD2IDENTIFIER);
        this.headerDebugResultsShort.add(this.RECORD2IDENTIFIER);

        this.comparatorLog.addAttribute(this.TOTALSIMILARITY);
        this.headerDebugResults.add(this.TOTALSIMILARITY);

        this.comparatorLog.addAttribute(this.ATTRIBUTE_IS_MATCH);
        this.headerDebugResults.add(this.ATTRIBUTE_IS_MATCH);

        this.comparatorLogShort.addAttribute(IComparatorLogger.COMPARATORNAME);
        this.headerDebugResultsShort.add(IComparatorLogger.COMPARATORNAME);

        this.comparatorLogShort.addAttribute(IComparatorLogger.RECORD1VALUE);
        this.headerDebugResultsShort.add(IComparatorLogger.RECORD1VALUE);

        this.comparatorLogShort.addAttribute(IComparatorLogger.RECORD2VALUE);
        this.headerDebugResultsShort.add(IComparatorLogger.RECORD2VALUE);

        this.comparatorLogShort.addAttribute(IComparatorLogger.RECORD1PREPROCESSEDVALUE);
        this.headerDebugResultsShort.add(IComparatorLogger.RECORD1PREPROCESSEDVALUE);

        this.comparatorLogShort.addAttribute(IComparatorLogger.RECORD2PREPROCESSEDVALUE);
        this.headerDebugResultsShort.add(IComparatorLogger.RECORD2PREPROCESSEDVALUE);

        this.comparatorLogShort.addAttribute(IComparatorLogger.SIMILARITY);
        this.headerDebugResultsShort.add(IComparatorLogger.SIMILARITY);

        this.comparatorLogShort.addAttribute(IComparatorLogger.POSTPROCESSEDSIMILARITY);
        this.headerDebugResultsShort.add(IComparatorLogger.POSTPROCESSEDSIMILARITY);

        this.resultToComparatorLog = new HashMap<Attribute, Attribute>();
        this.comparatorToResultLog = new HashMap<String, Attribute>();

    }

    public Aligner<SchemaElementType, Matchable> getAlignerForComparator(
            Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable, RecordType record1,
            RecordType record2, IComparator<RecordType, SchemaElementType> iComparator) {
        if (alignerProcessable != null) {
            Processable<Aligner<SchemaElementType, Matchable>> matchingSchemaAligner = alignerProcessable

                    .where((c) -> c.getFirstRecordType().getDataSourceIdentifier() == record1.getDataSourceIdentifier()
                            && c.getSecondRecordType().getDataSourceIdentifier() == record2.getDataSourceIdentifier())

                    .where((c) -> (iComparator.getFirstSchemaElement(record1) == null
                            || iComparator.getFirstSchemaElement(record1).equals(c.getFirstRecordType()))
                            && (iComparator.getSecondSchemaElement(record2) == null
                            || iComparator.getSecondSchemaElement(record2).equals(c.getSecondRecordType())));

            return matchingSchemaAligner.firstOrNull();
        } else {
            return null;
        }
    }

    protected void addComparatorToLog(IComparator<RecordType, SchemaElementType> iComparator) {

        int position = (this.comparatorLog.getSchema().size() - 4) / IComparatorLogger.COMPARATORLOG.length;

        for (Attribute att : IComparatorLogger.COMPARATORLOG) {
            String schemaIdentifier = String.format("[%d] %s %s", position, iComparator.getName(null).trim(), att.getIdentifier());

            Attribute schemaAttribute = new Attribute(schemaIdentifier);
            this.resultToComparatorLog.put(schemaAttribute, att);
            this.comparatorToResultLog.put(schemaIdentifier, schemaAttribute);
            this.comparatorLog.getSchema().add(schemaAttribute);
            if (!att.getIdentifier().equals(IComparatorLogger.COMPARATORNAME.getIdentifier())) {
                this.headerDebugResults.add(schemaAttribute);
            }
        }
    }

    protected Record fillDebugRecord(Record debug, IComparator<RecordType, SchemaElementType> iComparator, int position) {
        IComparatorLogger compLog = iComparator.getComparisonLog();
        if (compLog != null) {
            for (Attribute att : IComparatorLogger.COMPARATORLOG) {
                String identifier = String.format("[%d] %s %s", position, iComparator.getName(null).trim(), att.getIdentifier());
                Attribute schemaAtt = comparatorToResultLog.get(identifier);

                if (att == IComparatorLogger.RECORD1PREPROCESSEDVALUE) {
                    debug.setValue(schemaAtt, compLog.getRecord1PreprocessedValue());
                } else if (att == IComparatorLogger.RECORD2PREPROCESSEDVALUE) {
                    debug.setValue(schemaAtt, compLog.getRecord2PreprocessedValue());
                } else if (att == IComparatorLogger.POSTPROCESSEDSIMILARITY) {
                    debug.setValue(schemaAtt, compLog.getPostprocessedSimilarity());
                } else {
                    debug.setValue(schemaAtt, compLog.getValue(att));
                }
            }
        } else {
            log.error("A comparator's log is not defined!");
            log.error(
                    "Please check whether logging was enabled before the comparators were added to the matching rule!");
        }
        return debug;
    }

    protected void addDebugRecordShort(RecordType record1, RecordType record2,
                                       IComparator<RecordType, SchemaElementType> iComparator, int position) {
        Record debug = initializeDebugRecord(record1, record2, position);
        IComparatorLogger compLog = iComparator.getComparisonLog();
        if (compLog != null) {
            debug.setValue(IComparatorLogger.COMPARATORNAME, compLog.getComparatorName());
            debug.setValue(IComparatorLogger.RECORD1VALUE, compLog.getRecord1Value());
            debug.setValue(IComparatorLogger.RECORD2VALUE, compLog.getRecord2Value());
            debug.setValue(IComparatorLogger.RECORD1PREPROCESSEDVALUE, compLog.getRecord1PreprocessedValue());
            debug.setValue(IComparatorLogger.RECORD2PREPROCESSEDVALUE, compLog.getRecord2PreprocessedValue());
            debug.setValue(IComparatorLogger.SIMILARITY, compLog.getPostprocessedSimilarity());
            debug.setValue(IComparatorLogger.POSTPROCESSEDSIMILARITY, compLog.getPostprocessedSimilarity());

            this.comparatorLogShort.add(debug);

        } else {
            log.error("A comparator's log is not defined!");
            log.error(
                    "Please check whether logging was enabled before the comparators were added to the matching rule!");
        }
    }

    protected void fillSimilarity(RecordType record1, RecordType record2, double similarity) {
        String identifier = record1.getIdentifier() + "-" + record2.getIdentifier();
        Record debug = this.comparatorLog.getRecord(identifier);
        if(debug != null){
            debug.setValue(TOTALSIMILARITY, Double.toString(similarity));
        }
    }

    public void activateDebugReport(String filePath, int maxSize, BestMatchStandard bestMatchStandard){
        if(filePath != null && filePath.endsWith(".csv")){
            this.filePathDebugResults = filePath;
            this.maxDebugLogSize = maxSize;
            this.setCollectDebugResults(true);
            this.bestMatchStandard = bestMatchStandard;

            log.info("Activated Debug Report.");
        }
        else{
            log.error("Failed to activate Debug Report.");
            log.error("Please provide a valid path to a .csv file!");
        }
    }
    protected Record initializeDebugRecord(RecordType record1, RecordType record2, int position) {

        String identifier = record1.getIdentifier() + "-" + record2.getIdentifier();
        if (position != -1) {
            identifier = Integer.toString(position) + identifier;
        }
        Record debug = new Record(identifier);
        debug.setValue(this.MATCHINGRULE, getClass().getSimpleName());
        debug.setValue(this.RECORD1IDENTIFIER, record1.getIdentifier());
        debug.setValue(this.RECORD2IDENTIFIER, record2.getIdentifier());

        return debug;
    }
    protected void fillSimilarity(Record debug, Double similarity) {
        if (similarity != null) {
            debug.setValue(TOTALSIMILARITY, Double.toString(similarity));
        }

        this.comparatorLog.add(debug);

    }

    public void writeDebugMatchingResultsToFile(){
        if(this.bestMatchStandard != null){
            addBestMatchToDebugResults();
        }
        if (this.comparatorLog != null && this.comparatorLogShort != null
                && this.filePathDebugResults != null && this.filePathDebugResults.endsWith(".csv")) {
            try {
                new RecordCSVFormatter().writeCSV(new File(this.filePathDebugResults), this.comparatorLog, this.headerDebugResults);
                log.info("Debug results written to file: " + this.filePathDebugResults);


            } catch (IOException e) {
                log.error("Debug results could not be written to file: " + this.filePathDebugResults);
            }

            String filePathShortDebugResults = this.filePathDebugResults.replaceAll(".csv$", "_short.csv");

            try{
                new RecordCSVFormatter().writeCSV(new File(filePathShortDebugResults), this.comparatorLogShort,
                        this.headerDebugResultsShort);
                log.info("Short debug results written to file: " + filePathShortDebugResults);
            } catch (IOException e) {
                log.error("Short debug results could not be written to file: " + filePathShortDebugResults);
            }
        } else {
            log.error("No debug results found!");
            log.error("Is logging enabled?");
        }
    }

    public void setComparatorLog(MixedHashedDataSet<Record, Attribute> comparatorLog) {
        this.comparatorLog = comparatorLog;
    }

    protected void addBestMatchToDebugResults(){
        if (this.comparatorLog != null && this.comparatorLogShort != null && bestMatchStandard != null) {
            Boolean no_debug_record_found = true;
            for(Pair<String, String> pair: this.bestMatchStandard.getPositiveSamples()){
                String identifier = pair.getFirst()+ "-" + pair.getSecond();
                Record debug = this.comparatorLog.getRecord(identifier);
                if(debug != null){
                    debug.setValue(ATTRIBUTE_IS_MATCH, "1");
                    no_debug_record_found = false;
                }
            }

            for(Pair<String, String> pair: this.bestMatchStandard.getNegativeSamples()){
                String identifier = pair.getFirst()+ "-" + pair.getSecond();
                Record debug = this.comparatorLog.getRecord(identifier);
                if(debug != null){
                    debug.setValue(ATTRIBUTE_IS_MATCH, "0");
                    no_debug_record_found = false;
                }
            }

            if(no_debug_record_found){
                log.warn("No corresponding record for the Debug Log found in the Goldstandard!");
                log.warn("Please align the order of Data Sets in Goldstandard and Matching Rule!");
            }

        }
    }
    public void activateDebugReport(String filePath, int maxSize){
        this.activateDebugReport(filePath, maxSize, null);
    }


}
