package com.integration.em.matches;

import com.integration.em.aggregator.AlignedAggregator;
import com.integration.em.aggregator.TopKVotesAggregator;
import com.integration.em.blocker.Blocker;
import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.processing.Processable;
import com.integration.em.similarity.VotingMatchingRule;
import com.integration.em.tables.Pair;
import com.integration.em.utils.Q;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.time.Duration;
import java.time.LocalDateTime;

@Slf4j
public class DuplicateBasedMatchingAlgorithm<RecordType extends Matchable, SchemaElementType extends Matchable> implements MatchingAlgorithm<SchemaElementType, RecordType> {

    private DataSet<SchemaElementType, SchemaElementType> dataset1;
    private DataSet<SchemaElementType, SchemaElementType> dataset2;
    private Processable<Aligner<RecordType, Matchable>> alignerProcessable;
    private VotingMatchingRule<SchemaElementType, RecordType> rule;
    private TopKVotesAggregator<SchemaElementType, RecordType> voteFilter;
    private AlignedAggregator<SchemaElementType, RecordType> voting;
    private Blocker<SchemaElementType, SchemaElementType, SchemaElementType, RecordType> blocker;
    private Processable<Aligner<SchemaElementType, RecordType>> result;

    public DuplicateBasedMatchingAlgorithm(DataSet<SchemaElementType, SchemaElementType> dataset1, DataSet<SchemaElementType, SchemaElementType> dataset2, Processable<Aligner<RecordType, Matchable>> alignerProcessable, VotingMatchingRule<SchemaElementType, RecordType> rule, TopKVotesAggregator<SchemaElementType, RecordType> voteFilter, AlignedAggregator<SchemaElementType, RecordType> voting, Blocker<SchemaElementType, SchemaElementType, SchemaElementType, RecordType> blocker) {
        this.dataset1 = dataset1;
        this.dataset2 = dataset2;
        this.alignerProcessable = alignerProcessable;
        this.rule = rule;
        this.voteFilter = voteFilter;
        this.voting = voting;
        this.blocker = blocker;
    }

    public DuplicateBasedMatchingAlgorithm(DataSet<SchemaElementType, SchemaElementType> dataset1, DataSet<SchemaElementType, SchemaElementType> dataset2, Processable<Aligner<RecordType, Matchable>> alignerProcessable, VotingMatchingRule<SchemaElementType, RecordType> rule, AlignedAggregator<SchemaElementType, RecordType> voting, Blocker<SchemaElementType, SchemaElementType, SchemaElementType, RecordType> blocker) {
        this.dataset1 = dataset1;
        this.dataset2 = dataset2;
        this.alignerProcessable = alignerProcessable;
        this.rule = rule;
        this.voting = voting;
        this.blocker = blocker;
    }

    public DataSet<SchemaElementType, SchemaElementType> getDataset1() {
        return dataset1;
    }

    public DataSet<SchemaElementType, SchemaElementType> getDataset2() {
        return dataset2;
    }

    public Processable<Aligner<RecordType, Matchable>> getAlignerProcessable() {
        return alignerProcessable;
    }

    public VotingMatchingRule<SchemaElementType, RecordType> getRule() {
        return rule;
    }

    public Blocker<SchemaElementType, SchemaElementType, SchemaElementType, RecordType> getBlocker() {
        return blocker;
    }

    @Override
    public void run() {

        LocalDateTime start = LocalDateTime.now();

        log.info(String.format("[%s] Starting Duplicate-based Schema Matching",
                start.toString()));

        log.info(String.format("Blocking %,d x %,d elements", getDataset1().size(), getDataset2().size()));

        Processable<Aligner<SchemaElementType, RecordType>> blocked = runBlocking(getDataset1(), getDataset2(), Aligner.toMatchable(getAlignerProcessable()));

        log.info(String
                .format("Matching %,d x %,d elements; %,d blocked pairs (reduction ratio: %s)",
                        getDataset1().size(), getDataset2().size(),
                        blocked.size(), Double.toString(getBlocker().getReductionRatio())));

        Processable<Pair<Pair<SchemaElementType, SchemaElementType>, Aligner<SchemaElementType, RecordType>>> aggregatedVotes;
        if(voteFilter==null) {
            aggregatedVotes = blocked.aggregate(rule, voting);
        } else {
            // vote
            Processable<Aligner<SchemaElementType, RecordType>> votes = blocked.map(rule);

            Processable<Pair<Pair<SchemaElementType, RecordType>, Processable<Aligner<SchemaElementType, RecordType>>>> filteredVotes = votes.aggregate((r,c) ->
            {
                Aligner<RecordType, Matchable> cause = Q.firstOrDefault(r.getCausalAligners().get());
                if(cause!=null) {
                    c.next(new Pair<Pair<SchemaElementType, RecordType>, Aligner<SchemaElementType, RecordType>>(
                            new Pair<SchemaElementType, RecordType>(r.getFirstRecordType(), cause.getFirstRecordType()),
                            r
                    ));
                }
            }, voteFilter);

            // aggregate the votes
            aggregatedVotes = filteredVotes.aggregate((r,c) -> {
                if(r!=null) {
                    for(Aligner<SchemaElementType, RecordType> alg : r.getSecond().get()) {
                        c.next(new Pair<>(new Pair<>(alg.getFirstRecordType(), alg.getSecondRecordType()), alg));
                    }
                }
            }, voting);

        }

        result = aggregatedVotes.map((p,c) -> {
            if(p.getSecond()!=null) {
                c.next(p.getSecond());
            }
        });

        // report total matching time
        LocalDateTime end = LocalDateTime.now();

        log.info(String.format(
                "Duplicate-based Schema Matching finished after %s; found %d correspondences from %,d duplicates.",
                DurationFormatUtils.formatDurationHMS(Duration.between(start, end).toMillis()), result.size(), getAlignerProcessable().size()));
    }

    @Override
    public Processable<Aligner<SchemaElementType, RecordType>> getResult() {
        return result;
    }

    public Processable<Aligner<SchemaElementType, RecordType>> runBlocking(DataSet<SchemaElementType, SchemaElementType> dataset1, DataSet<SchemaElementType, SchemaElementType> dataset2, Processable<Aligner<RecordType, Matchable>> alignerProcessable) {
        return getBlocker().createBlocking(getDataset1(), getDataset2(), getAlignerProcessable());
    }
}
