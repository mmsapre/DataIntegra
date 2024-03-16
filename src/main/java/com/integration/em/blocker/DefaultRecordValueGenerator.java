package com.integration.em.blocker;

import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.*;
import com.integration.em.processing.DataIterator;
import com.integration.em.processing.Processable;
import com.integration.em.processing.ProcessableCollection;
import com.integration.em.tables.Pair;
import com.integration.em.model.Record;

public class DefaultRecordValueGenerator extends BlockingKeyGenerator<Record, MatchableValue, Record> {

	private static final long serialVersionUID = 1L;
	DataSet<Attribute, Attribute> schema;
	
	public DefaultRecordValueGenerator(DataSet<Attribute, Attribute> schema) {
		this.schema = schema;
	}

	@Override
	public void mapRecordToKey(Pair<Record, Processable<Aligner<MatchableValue, Matchable>>> pair, DataIterator<Pair<String, Pair<Record, Processable<Aligner<MatchableValue, Matchable>>>>> resultCollector) {

		Record record = pair.getFirst();
		for(Attribute a : schema.get()) {
			if(record.hasValue(a)) {
				
				Processable<Aligner<MatchableValue, Matchable>> causes = new ProcessableCollection<>();
				MatchableValue value = new MatchableValue(record.getValue(a), record.getIdentifier(), a.getIdentifier());
				Aligner<MatchableValue, Matchable> causeCor = new Aligner<>(value, value, 1.0);
				causes.add(causeCor);
				
				resultCollector.next(new Pair<>(value.getValue().toString(), new Pair<>(record, causes)));
			}
		}
		
	}


	@Override
	public void generateBlockingKeys(Record record, Processable<Aligner<MatchableValue, Matchable>> alignerProcessable,
			DataIterator<Pair<String, Record>> resultCollector) {
	}

}
