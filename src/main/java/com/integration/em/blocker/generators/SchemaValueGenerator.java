package com.integration.em.blocker.generators;

import com.integration.em.model.Aligner;
import com.integration.em.model.Matchable;
import com.integration.em.model.MatchableValue;
import com.integration.em.processing.DataIterator;
import com.integration.em.processing.Processable;
import com.integration.em.processing.ProcessableCollection;
import com.integration.em.processing.ProcessableCollector;
import com.integration.em.tables.Pair;

public abstract class SchemaValueGenerator<RecordType extends Matchable, SchemaElementType extends Matchable> extends BlockingKeyGenerator<RecordType, MatchableValue, SchemaElementType> {

    @Override
    public void mapRecordToKey(Pair<RecordType, Processable<Aligner<MatchableValue, Matchable>>> pair,
                               DataIterator<Pair<String, Pair<SchemaElementType, Processable<Aligner<MatchableValue, Matchable>>>>> resultCollector) {


        ProcessableCollector<Pair<String, SchemaElementType>> collector = new ProcessableCollector<>();
        collector.setResult(new ProcessableCollection<>());
        collector.initialise();

        generateBlockingKeys(pair.getFirst(), pair.getSecond(), collector);

        collector.finalise();

        for(Pair<String, SchemaElementType> p : collector.getResult().get()) {

            Processable<Aligner<MatchableValue, Matchable>> causes = new ProcessableCollection<>();
            MatchableValue matchableValue = new MatchableValue(p.getFirst(), pair.getFirst().getIdentifier(), p.getSecond().getIdentifier());
            Aligner<MatchableValue, Matchable> causeCor = new Aligner<>(matchableValue, matchableValue, 1.0);
            causes.add(causeCor);

            resultCollector.next(new Pair<>(matchableValue.getValue().toString(), new Pair<>(p.getSecond(), causes)));

        }

    }
}
