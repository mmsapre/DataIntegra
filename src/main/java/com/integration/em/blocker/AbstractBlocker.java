package com.integration.em.blocker;

import com.integration.em.model.*;
import com.integration.em.model.Record;
import com.integration.em.processing.Group;
import com.integration.em.processing.Processable;
import com.integration.em.processing.ProcessableCollection;
import com.integration.em.processing.RecordKeyValueMapper;
import com.integration.em.tables.Pair;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public abstract class AbstractBlocker<RecordType extends Matchable, BlockedType extends Matchable, AlignerType extends Matchable> {

    private double reductionRatio = 1.0;
    private MixedHashedDataSet<Record, Attribute> debugBlockingResults;
    private List<Attribute> headerDebugResults;

    public static final Attribute frequency = new Attribute("Frequency");
    public static final Attribute blockingKeyValue = new Attribute("Blocking Key Value");

    private String filePathDebugResults;
    private int maxDebugLogSize;

    private boolean measureBlockSizes = false;
    public void setMeasureBlockSizes(boolean measureBlockSizes) {
        this.measureBlockSizes = measureBlockSizes;
        if(this.measureBlockSizes){
            this.initializeBlockingResults();
        }
    }


    public boolean isMeasureBlockSizes() {
        return measureBlockSizes;
    }



    public double getReductionRatio() {
        return reductionRatio;
    }

    private Processable<Aligner<BlockedType, AlignerType>> result;

    protected void setResult(Processable<Aligner<BlockedType, AlignerType>> result) {
        this.result = result;
    }


    public Processable<Aligner<BlockedType, AlignerType>> getBlockedPairs() {
        return result;
    }


    protected void calculatePerformance(Processable<? extends Matchable> dataset1,
                                        Processable<? extends Matchable> dataset2,
                                        Processable<? extends Aligner<? extends Matchable, ? extends Matchable>> blocked) {
        long size1 = (long) dataset1.size();
        long size2 = (long) dataset2.size();
        long maxPairs = size1 * size2;

        reductionRatio = 1.0 - ((double) blocked.size() / (double) maxPairs);
    }

    public Processable<Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>>> combineDataWithAligner(
            Processable<RecordType> dataset1,
            Processable<Aligner<AlignerType, Matchable>> schemaCorrespondences,
            RecordKeyValueMapper<Object, Aligner<AlignerType, Matchable>, Aligner<AlignerType, Matchable>> correspondenceJoinKey) {

        if (schemaCorrespondences != null) {
            Processable<Group<Object, Aligner<AlignerType, Matchable>>> leftCors = schemaCorrespondences
                    .group(correspondenceJoinKey);

            Processable<Pair<RecordType, Group<Object, Aligner<AlignerType, Matchable>>>> joined = dataset1
                    .leftJoin(leftCors, (r) -> r.getDataSourceIdentifier(), (r) -> r.getKey());

            return joined.map((p, c) -> {
                if (p.getSecond() != null) {
                    c.next(new Pair<>(p.getFirst(), p.getSecond().getRecords()));
                } else {
                    c.next(new Pair<>(p.getFirst(), null));
                }

            });
        } else {
            return dataset1.map((r, c) -> c.next(new Pair<>(r, null)));
        }
    }

    protected Processable<Aligner<AlignerType, Matchable>> createCausalCorrespondences(
            Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>> p1,
            Pair<RecordType, Processable<Aligner<AlignerType, Matchable>>> p2) {
        return new ProcessableCollection<>(p1.getSecond()).append(p2.getSecond()).distinct();
    }


    public void initializeBlockingResults() {
        this.debugBlockingResults = new MixedHashedDataSet<Record, Attribute>();
        this.headerDebugResults = new LinkedList<Attribute>();

        this.debugBlockingResults.addAttribute(AbstractBlocker.blockingKeyValue);
        this.debugBlockingResults.addAttribute(AbstractBlocker.frequency);

        this.headerDebugResults.add(AbstractBlocker.blockingKeyValue);
        this.headerDebugResults.add(AbstractBlocker.frequency);
    }


    public void appendBlockingResult(Record model) {
        if(this.maxDebugLogSize == -1 || this.debugBlockingResults.size() < this.maxDebugLogSize){
            this.debugBlockingResults.add(model);
        }
    }


    public void collectBlockSizeData(String filePath, int maxSize){
        if(filePath != null){
            this.filePathDebugResults = filePath;
            this.maxDebugLogSize = maxSize;
            this.setMeasureBlockSizes(true);
        }
    }


    public void writeDebugBlockingResultsToFile() {
        if(this.debugBlockingResults != null){
            try {
                new RecordCSVFormatter().writeCSV(new File(this.filePathDebugResults), this.debugBlockingResults, this.headerDebugResults);
            } catch (IOException e) {
                log.error("Debug results could not be written to file: " + this.filePathDebugResults);
            }
            log.info("Debug results written to file: " + this.filePathDebugResults);
        }else{
            log.error("No debug results for blocking found!");
            log.error("Is logging enabled?");
        }
    }
}
