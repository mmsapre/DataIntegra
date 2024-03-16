package com.integration.em.merge;

import com.integration.em.model.*;
import com.integration.em.processing.Processable;
import com.integration.em.processing.ProcessableCollection;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.*;
import com.integration.em.model.Record;

@Slf4j
public class MixedDataStrategy<RecordType extends Matchable & Mixed<SchemaElementType>, SchemaElementType extends Matchable> {

    private Map<SchemaElementType, AttributeMixer<RecordType, SchemaElementType>> attributeMixerMap;
    private Map<SchemaElementType, EvaluationRule<RecordType, SchemaElementType>> evaluationRuleMap;

    private MixedDataFactory<RecordType,SchemaElementType> mixedDataFactory;

    private MixedHashedDataSet<Record, Attribute> mixedHashedDataSet;

    private boolean collectDebugResults = false;
    private List<Attribute> headerDebugResults;
    private DataSet<RecordType, SchemaElementType> bestForDebug;

    private String filePathDebugResults;
    private int	maxDebugLogSize;

    public MixedDataStrategy(MixedDataFactory<RecordType, SchemaElementType> mixedDataFactory) {
        attributeMixerMap = new HashMap<>();
        evaluationRuleMap = new HashMap<>();
        this.mixedDataFactory = mixedDataFactory;
    }

    public MixedDataSet<RecordType, SchemaElementType> createMixedDataSet() {
        MixedDataSet<RecordType, SchemaElementType> mixedDataSet = new MixedHashedDataSet<>();
        for(SchemaElementType attribute : attributeMixerMap.keySet()) {
            mixedDataSet.addAttribute(attribute);
        }
        return mixedDataSet;
    }

    protected void initializeMixedResults() {
        this.mixedHashedDataSet = new MixedHashedDataSet<Record,Attribute>();
        this.headerDebugResults = new LinkedList<Attribute>();

        this.mixedHashedDataSet.addAttribute(AttributeMixedLogger.ATTRIBUTE_NAME);
        this.headerDebugResults.add(AttributeMixedLogger.ATTRIBUTE_NAME);

        this.mixedHashedDataSet.addAttribute(AttributeMixedLogger.CONSISTENCY);
        this.headerDebugResults.add(AttributeMixedLogger.CONSISTENCY);

        this.mixedHashedDataSet.addAttribute(AttributeMixedLogger.VALUEIDS);
        this.headerDebugResults.add(AttributeMixedLogger.VALUEIDS);

        this.mixedHashedDataSet.addAttribute(AttributeMixedLogger.RECORDIDS);
        this.headerDebugResults.add(AttributeMixedLogger.RECORDIDS);

        this.mixedHashedDataSet.addAttribute(AttributeMixedLogger.VALUES);
        this.headerDebugResults.add(AttributeMixedLogger.VALUES);

        this.mixedHashedDataSet.addAttribute(AttributeMixedLogger.MIXEDVALUE);
        this.headerDebugResults.add(AttributeMixedLogger.MIXEDVALUE);

        this.mixedHashedDataSet.addAttribute(AttributeMixedLogger.IS_CORRECT);
        this.headerDebugResults.add(AttributeMixedLogger.IS_CORRECT);

        this.mixedHashedDataSet.addAttribute(AttributeMixedLogger.CORRECT_VALUE);
        this.headerDebugResults.add(AttributeMixedLogger.CORRECT_VALUE);
    }
    public void addAttributeFuser(SchemaElementType schemaElement, AttributeMixer<RecordType, SchemaElementType> attributeMixer, EvaluationRule<RecordType, SchemaElementType> rule) {
        if(this.collectDebugResults){
            attributeMixer.setDebugResults(true);
        }
        attributeMixerMap.put(schemaElement, attributeMixer);
        evaluationRuleMap.put(schemaElement, rule);
    }

    public RecordType apply(RecordGroup<RecordType, SchemaElementType> group, Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable) {
        RecordType forMixed = mixedDataFactory.createInstanceForMixed(group);

        for (AttributeMixedTask<RecordType, SchemaElementType> t : getAttributesMix(group, alignerProcessable)) {
            t.execute(group, forMixed);

        }

        return forMixed;
    }

    public List<AttributeMixedTask<RecordType, SchemaElementType>> getAttributesMix(RecordGroup<RecordType, SchemaElementType> group, Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable) {
        List<AttributeMixedTask<RecordType, SchemaElementType>> attributeMixedTasks = new ArrayList<>();

        if(alignerProcessable!=null) {
            Map<SchemaElementType, Processable<Aligner<SchemaElementType, Matchable>>> byTargetSchema = new HashMap<>();

            for(Aligner<SchemaElementType, Matchable> aligner : alignerProcessable.get()) {

                Processable<Aligner<SchemaElementType, Matchable>> processable = byTargetSchema.get(aligner.getSecondRecordType());

                if(processable==null) {
                    processable = new ProcessableCollection<>();
                    byTargetSchema.put(aligner.getSecondRecordType(), processable);
                }

                processable.add(aligner);
            }

            for(SchemaElementType elem : byTargetSchema.keySet()) {
                AttributeMixedTask<RecordType, SchemaElementType> t = new AttributeMixedTask<>();
                t.setSchemaElement(elem);
                t.setAttributeMixer(attributeMixerMap.get(elem));
                t.setAlignerProcessable(byTargetSchema.get(elem));
                t.setEvaluationRule(evaluationRuleMap.get(elem));
                attributeMixedTasks.add(t);
            }
        } else {
            for(SchemaElementType elem : attributeMixerMap.keySet()) {
                AttributeMixedTask<RecordType, SchemaElementType> t = new AttributeMixedTask<>();
                t.setSchemaElement(elem);
                t.setAttributeMixer(attributeMixerMap.get(elem));
                t.setEvaluationRule(evaluationRuleMap.get(elem));
                attributeMixedTasks.add(t);
            }
        }

        return attributeMixedTasks;
    }

    public Map<String, Double> getAttributeConsistency(RecordGroup<RecordType, SchemaElementType> group, Processable<Aligner<SchemaElementType, Matchable>> schemaCorrespondences) {
        Map<String, Double> consistencies = new HashMap<>();

        List<AttributeMixedTask<RecordType, SchemaElementType>> tasks = getAttributesMix(group, schemaCorrespondences);

        for (AttributeMixedTask<RecordType, SchemaElementType> attributeMixedTask : tasks) {

            AttributeMixer<RecordType, SchemaElementType> attributeMixer = attributeMixedTask.getAttributeMixer();

            EvaluationRule<RecordType, SchemaElementType> rule = attributeMixedTask.getEvaluationRule();

            if(attributeMixer!=null || rule!=null) {

                Double consistency = attributeMixer.getConsistency(group, rule, attributeMixedTask.getAlignerProcessable(), attributeMixedTask.getSchemaElement());

                if(consistency!=null) {
                    consistencies.put(attributeMixedTask.getSchemaElement().getIdentifier(), consistency);
                }
            }
        }

        return consistencies;
    }

    protected void calculateRecordLevelDebugResultsAndWriteToFile(MixedDataSet<RecordType, SchemaElementType> mixedDataSet){
        if(this.mixedHashedDataSet != null) {
            MixedHashedDataSet<Record, Attribute> debugFusionResultsRecordLevel = new MixedHashedDataSet<Record, Attribute>();
            List<Attribute> headerDebugResultsRecordLevel = new LinkedList<Attribute>();

            // Initialise Attributes
            Attribute attributeRecordIDS = new Attribute("RecordIDS");
            debugFusionResultsRecordLevel.addAttribute(attributeRecordIDS);
            headerDebugResultsRecordLevel.add(attributeRecordIDS);

            Attribute attributeAvgConsistency = new Attribute("AverageConsistency");
            debugFusionResultsRecordLevel.addAttribute(attributeAvgConsistency);
            headerDebugResultsRecordLevel.add(attributeAvgConsistency);

            Set<String> attributeSet = new HashSet<String>();
            HashMap<String, Attribute> attributeHashMap = new HashMap<String, Attribute>();
            Set<String> recordsIDSet = new HashSet<String>();

            for (Record record : this.mixedHashedDataSet.get()){
                String attributeName = record.getValue(AttributeMixedLogger.ATTRIBUTE_NAME);
                if(!attributeSet.contains(attributeName)){
                    attributeSet.add(attributeName);
                    Attribute attributeConsistency = new Attribute(attributeName + "-Consistency");
                    debugFusionResultsRecordLevel.addAttribute(attributeConsistency);
                    headerDebugResultsRecordLevel.add(attributeConsistency);
                    attributeHashMap.put(attributeName + "-Consistency", attributeConsistency);

                    Attribute attributeValues = new Attribute(attributeName + "-Values");
                    debugFusionResultsRecordLevel.addAttribute(attributeValues);
                    headerDebugResultsRecordLevel.add(attributeValues);
                    attributeHashMap.put(attributeName + "-Values", attributeValues);
                }
                recordsIDSet.add(record.getValue(AttributeMixedLogger.RECORDIDS));
            }

            for (String recordIDs: recordsIDSet){

                String [] originalIDS = recordIDs.split("\\+");
                RecordType fusedRecord = mixedDataSet.getRecord(originalIDS[0]);
                String fusedRecordIdentifier = fusedRecord.getIdentifier();

                Record record = debugFusionResultsRecordLevel.getRecord(fusedRecordIdentifier);
                if (record == null){
                    record = new Record(fusedRecord.getIdentifier());
                    record.setValue(attributeRecordIDS, fusedRecord.getIdentifier());
                }

                for (String attributeName: attributeSet){
                    String recordIdentifier = attributeName + "-{" + recordIDs + "}";
                    Record debugRecord = this.mixedHashedDataSet.getRecord(recordIdentifier);
                    if(debugRecord != null){
                        Attribute attributeConsistency = attributeHashMap.get(attributeName + "-Consistency");
                        String consistency = debugRecord.getValue(AttributeMixedLogger.CONSISTENCY);
                        record.setValue(attributeConsistency, consistency);

                        Attribute attributeValues = attributeHashMap.get(attributeName + "-Values");
                        String values = debugRecord.getValue(AttributeMixedLogger.VALUES);
                        record.setValue(attributeValues, values);
                    }
                }
                debugFusionResultsRecordLevel.add(record);
            }

            for(Record debugRecord: debugFusionResultsRecordLevel.get()){
                double sumConsistencies = 0;
                int countAttributes = 0;
                for (String attributeName: attributeSet){
                    Attribute attributeConsistency = attributeHashMap.get(attributeName + "-Consistency");
                    String consistency = debugRecord.getValue(attributeConsistency);
                    if (consistency != null){
                        sumConsistencies = sumConsistencies + Double.parseDouble(consistency);
                        countAttributes++;
                    }
                }
                double avgConsistency = sumConsistencies/countAttributes;
                debugRecord.setValue(attributeAvgConsistency, Double.toString(avgConsistency));
            }

            String debugReportfilePath = this.filePathDebugResults.replaceAll(".csv$", "_recordLevel.csv");
            try {
                new RecordCSVFormatter().writeCSV(new File(debugReportfilePath), debugFusionResultsRecordLevel, headerDebugResultsRecordLevel);
                log.info("Debug results on record level written to file: " + debugReportfilePath);
            } catch (IOException e) {
                log.error("Debug results on record level could not be written to file: " + debugReportfilePath);
            }
        }
    }

    protected void writeDebugDataFusionResultsToFile(){
        if(this.mixedHashedDataSet != null){
            try {
                new RecordCSVFormatter().writeCSV(new File(this.filePathDebugResults), this.mixedHashedDataSet, this.headerDebugResults);
                log.info("Debug results written to file: " + this.filePathDebugResults);
            } catch (IOException e) {
                log.error("Debug results could not be written to file: " + this.filePathDebugResults);
            }
        } else {
            log.error("No debug results found!");
            log.error("Is logging enabled?");
        }
    }



    protected void fillMixedLog(AttributeMixedTask<RecordType, SchemaElementType> attributeMixedTask, RecordGroup<RecordType, SchemaElementType> recordGroup, Processable<Aligner<SchemaElementType, Matchable>> alignerProcessable, RecordType fusedRecord){
        AttributeMixer<RecordType, SchemaElementType> attributeMixer = attributeMixedTask.getAttributeMixer();
        if(attributeMixer.getAttributeMixedLogger() != null && (this.maxDebugLogSize == -1 || this.mixedHashedDataSet.size() < this.maxDebugLogSize)){
            AttributeMixedLogger record = attributeMixer.getAttributeMixedLogger();
            record.setAttributeName(attributeMixedTask.getSchemaElement().getIdentifier());
            Double consistency = attributeMixer.getConsistency(recordGroup, attributeMixedTask.getEvaluationRule(), alignerProcessable, attributeMixedTask.getSchemaElement());
            if(consistency!=null) {
                record.setConsistency(consistency);
            }
            if(bestForDebug!=null) {
                RecordType fusedInGs = null;
                for (RecordType recordGs : bestForDebug.get()) {
                    if(recordGs.getIdentifier().equals(fusedRecord.getIdentifier())){
                        fusedInGs = recordGs;
                        break;
                    }
                    else{
                        for(String inputRecordId: recordGroup.getRecordIds()){
                            if(recordGs.getIdentifier().equals(inputRecordId)){
                                fusedInGs = recordGs;
                                break;
                            }
                        }
                        if(fusedInGs != null){
                            break;
                        }
                    }
                }
                if(fusedInGs!=null) {
                    record.setIsCorrect(attributeMixedTask.getEvaluationRule().isEqual(fusedRecord, fusedInGs, attributeMixedTask.getSchemaElement()));
                    if(attributeMixer instanceof AttributeValueMixer) {
                        AttributeValueMixer attributeMixer1 = (AttributeValueMixer)attributeMixer;
                        Object value = attributeMixer1.getValue(fusedInGs, null);
                        record.setCorrectValue(value);
                    }
                }
            }
            this.mixedHashedDataSet.add(record);
        }
        //}
    }

}
