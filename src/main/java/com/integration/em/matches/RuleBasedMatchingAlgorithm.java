package com.integration.em.matches;


import com.integration.em.blocker.Blocker;
import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.processing.Processable;
import com.integration.em.rules.MatchingRule;
import lombok.extern.java.Log;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.time.Duration;
import java.time.LocalDateTime;

@Log
public class RuleBasedMatchingAlgorithm<RecordType extends Matchable, SchemaElementType extends Matchable, AlignerType extends Matchable> implements MatchingAlgorithm<RecordType, AlignerType> {

    private DataSet<RecordType, SchemaElementType> dataset1;
    private DataSet<RecordType, SchemaElementType> dataset2;
    private Processable<Aligner<AlignerType, Matchable>> alignerProcessable;
    private MatchingRule<RecordType, AlignerType> rule;
    private Blocker<RecordType, SchemaElementType, RecordType, AlignerType> blocker;
    private Processable<Aligner<RecordType, AlignerType>> result;
    private String taskName = "Matching";


    public RuleBasedMatchingAlgorithm(DataSet<RecordType, SchemaElementType> dataset1,
                                      DataSet<RecordType, SchemaElementType> dataset2,
                                      Processable<Aligner<AlignerType, Matchable>> alignerProcessable,
                                      MatchingRule<RecordType, AlignerType> rule,
                                      Blocker<RecordType, SchemaElementType, RecordType, AlignerType> blocker) {
        super();
        this.dataset1 = dataset1;
        this.dataset2 = dataset2;
        this.alignerProcessable = alignerProcessable;
        this.rule = rule;
        this.blocker = blocker;
    }

    public DataSet<RecordType, SchemaElementType> getDataset1() {
        return dataset1;
    }

    public DataSet<RecordType, SchemaElementType> getDataset2() {
        return dataset2;
    }

    public Processable<Aligner<AlignerType, Matchable>> getAlignerProcessable() {
        return alignerProcessable;
    }

    public MatchingRule<RecordType, AlignerType> getRule() {
        return rule;
    }

    public Blocker<RecordType, SchemaElementType, RecordType, AlignerType> getBlocker() {
        return blocker;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public double getReductionRatio() {
        return getBlocker().getReductionRatio();
    }

    @Override
    public void run() {

        LocalDateTime start = LocalDateTime.now();
        Processable<Aligner<RecordType, AlignerType>> allPairs = createBlocking(getDataset1(), getDataset2(), getAlignerProcessable());

        Processable<Aligner<RecordType, AlignerType>> result = allPairs.map(rule);
        LocalDateTime end = LocalDateTime.now();

        log.info(String.format(
                "%s finished after %s; found %,d correspondences.",
                getTaskName(), DurationFormatUtils.formatDurationHMS(Duration.between(start, end).toMillis()), result.size()));

        if(rule.isDebugReportActive()){
            rule.writeDebugMatchingResultsToFile();
        }

        this.result = result;
    }

    public void createBlocking(){
        this.result = createBlocking(getDataset1(), getDataset2(), getAlignerProcessable());
    }

    @Override
    public Processable<Aligner<RecordType, AlignerType>> getResult() {
        return this.result;
    }

    public Processable<Aligner<RecordType, AlignerType>> createBlocking(
            DataSet<RecordType, SchemaElementType> dataset1,
            DataSet<RecordType, SchemaElementType> dataset2,
            Processable<Aligner<AlignerType, Matchable>> alignerProcessable) {

        LocalDateTime start = LocalDateTime.now();

        log.info(String.format("Starting %s", getTaskName()));

        log.info(String.format("Blocking %,d x %,d elements", getDataset1().size(), getDataset2().size()));

        Processable<Aligner<RecordType, AlignerType>> pairs = blocker.createBlocking(getDataset1(),
                getDataset2(), getAlignerProcessable());

        LocalDateTime afterBlocking = LocalDateTime.now();
        log.info(String
                .format("Matching %,d x %,d elements after %s; %,d blocked pairs (reduction ratio: %s)",
                        getDataset1().size(), getDataset2().size(),
                        DurationFormatUtils.formatDurationHMS(Duration.between(start, afterBlocking).toMillis()),
                        pairs.size(), Double.toString(getReductionRatio())));

        if(blocker.isMeasureBlockSizes()){
            blocker.writeDebugBlockingResultsToFile();
        }

        return pairs;
    }
}
