package com.integration.em.blocker;


import com.integration.em.blocker.generators.BlockingKeyGenerator;
import com.integration.em.model.*;
import com.integration.em.processing.DataIterator;
import com.integration.em.processing.Processable;
import com.integration.em.tables.Pair;
import com.integration.em.model.Record;

public class DefaultAttributeValuesAsBlockingKeyGenerator extends BlockingKeyGenerator<Record, MatchableValue, Attribute> {

	private static final long serialVersionUID = 1L;
	DataSet<Attribute, Attribute> schema;
	
	public DefaultAttributeValuesAsBlockingKeyGenerator(DataSet<Attribute, Attribute> schema) {
		this.schema = schema;
	}


	@Override
	public void generateBlockingKeys(Record record, Processable<Aligner<MatchableValue, Matchable>> alignerProcessable,
			DataIterator<Pair<String, Attribute>> resultCollector) {

		for(Attribute a : schema.get()) {
			if(record.hasValue(a)) {
				resultCollector.next(new Pair<>(record.getValue(a), a));
			}
		}
		
	}

}
