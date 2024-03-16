package com.integration.em.matches;

import com.integration.em.aggregator.AlignedAggregator;
import com.integration.em.blocker.Blocker;
import com.integration.em.model.Aligner;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.model.MatchableValue;
import com.integration.em.processing.Processable;
import com.integration.em.rules.IdentifierMatchingRule;
import com.integration.em.rules.MatchingAlgo;
import com.integration.em.tables.Pair;

public class InstanceBasedSchemaMatching<RecordType extends Matchable, SchemaElementType extends Matchable> implements MatchingAlgo<SchemaElementType, MatchableValue> {

    private DataSet<RecordType, SchemaElementType> dataset1;
    private DataSet<RecordType, SchemaElementType> dataset2;
    private Blocker<RecordType, SchemaElementType, SchemaElementType, MatchableValue> blocker;
    private AlignedAggregator<SchemaElementType, MatchableValue> alignedAggregator;
    private Processable<Aligner<SchemaElementType, MatchableValue>> alignerProcessable;


    public InstanceBasedSchemaMatching(DataSet<RecordType, SchemaElementType> dataset1,
                                                DataSet<RecordType, SchemaElementType> dataset2,
                                                Blocker<RecordType, SchemaElementType, SchemaElementType, MatchableValue> blocker,
                                                AlignedAggregator<SchemaElementType, MatchableValue> aggregator) {
        this.dataset1 = dataset1;
        this.dataset2 = dataset2;
        this.blocker = blocker;
        this.alignedAggregator = aggregator;
    }
    @Override
    public void run() {

        Processable<Aligner<SchemaElementType, MatchableValue>> blocked = blocker.createBlocking(getDataset1(), getDataset2(), null);

        Processable<Pair<Pair<SchemaElementType, SchemaElementType>, Aligner<SchemaElementType, MatchableValue>>> aggregated = blocked.aggregate(new IdentifierMatchingRule<SchemaElementType, MatchableValue>(0.0), alignedAggregator);

        Processable<Aligner<SchemaElementType, MatchableValue>> result = aggregated.map((p, collector) -> {
            if(p.getSecond()!=null)
            {
                collector.next(p.getSecond());
            }
        });

        setResult(result);
    }

    public void setResult(Processable<Aligner<SchemaElementType, MatchableValue>> alignerProcessable) {
        this.alignerProcessable = alignerProcessable;
    }


    @Override
    public Processable<Aligner<SchemaElementType, MatchableValue>> getResult() {
        return alignerProcessable;
    }

    public DataSet<RecordType, SchemaElementType> getDataset1() {
        return dataset1;
    }

    public DataSet<RecordType, SchemaElementType> getDataset2() {
        return dataset2;
    }
}
