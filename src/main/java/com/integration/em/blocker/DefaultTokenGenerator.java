package com.integration.em.blocker;

import com.integration.em.blocker.generators.TokenGenerator;
import com.integration.em.model.Aligner;
import com.integration.em.model.Attribute;
import com.integration.em.model.DataSet;
import com.integration.em.model.Matchable;
import com.integration.em.processing.DataIterator;
import com.integration.em.processing.Processable;
import com.integration.em.tables.Pair;
import com.integration.em.model.Record;

public class DefaultTokenGenerator extends TokenGenerator<Record, Attribute> {

	private static final long serialVersionUID = 1L;
	DataSet<Attribute, Attribute> schema;

	public DefaultTokenGenerator(DataSet<Attribute, Attribute> schema) {
		this.schema = schema;
	}


	@Override
	public void generateTokens(Record record, Processable<Aligner<Attribute, Matchable>> alignerProcessable, DataIterator<Pair<String, Record>> resultCollector) {

		for(Attribute a : schema.get()) {
			if (record.hasValue(a)) {
				String[] tokens = tokenizeString(record.getValue(a));

				for (String token : tokens) {
					resultCollector.next(new Pair<>(token, record));
				}
			}
		}
	}

	@Override
	public String[] tokenizeString(String value) {
		return value.split(" ");
	}

}
