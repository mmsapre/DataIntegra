package com.integration.em.blocker;

import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.*;
import com.integration.em.processing.DataIterator;
import com.integration.em.processing.Processable;
import com.integration.em.tables.Pair;
import com.integration.em.model.Record;

public class DefaultRecordValuesAsBlockingKeyGenerator extends BlockingKeyGenerator<Record, MatchableValue, Record> {

	private static final long serialVersionUID = 1L;
	DataSet<Attribute, Attribute> schema;
	
	public DefaultRecordValuesAsBlockingKeyGenerator(DataSet<Attribute, Attribute> schema) {
		this.schema = schema;
	}

	@Override
	public void generateBlockingKeys(Record record, Processable<Aligner<MatchableValue, Matchable>> alignerProcessablel, DataIterator<Pair<String, Record>> resultCollector) {
		for(Attribute a : schema.get()) {
			if(record.hasValue(a)) {
				resultCollector.next(new Pair<>(record.getValue(a), record));
			}
		}
	}

}
