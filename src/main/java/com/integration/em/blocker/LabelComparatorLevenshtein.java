package com.integration.em.blocker;

import com.integration.em.model.Aligner;
import com.integration.em.model.Attribute;
import com.integration.em.model.Matchable;
import com.integration.em.similarity.LevenshteinSimilarity;
import com.integration.em.utils.Comparator;
import com.integration.em.utils.ComparatorLogger;
import com.integration.em.model.Record;

public class LabelComparatorLevenshtein implements Comparator<Attribute, Record> {

	private static final long serialVersionUID = 1L;

	private LevenshteinSimilarity similarity = new LevenshteinSimilarity();
	private ComparatorLogger comparisonLog;

	@Override
	public double compare(Attribute record1, Attribute record2, Aligner<Record, Matchable> schemaCorrespondence) {

		double sim = similarity.calculate(record1.getName(), record2.getName());
		if (this.comparisonLog != null) {
			this.comparisonLog.setComparatorName(getClass().getName());

			this.comparisonLog.setRecord1Value(record1.getName());
			this.comparisonLog.setRecord2Value(record2.getName());

			this.comparisonLog.setSimilarity(Double.toString(sim));
		}

		return sim;
	}

	@Override
	public ComparatorLogger getComparisonLog() {
		return this.comparisonLog;
	}

	@Override
	public void setComparisonLog(ComparatorLogger comparatorLog) {
		this.comparisonLog = comparatorLog;
	}

}
