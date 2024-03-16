
package com.integration.em.blocker;


import com.integration.em.model.Aligner;
import com.integration.em.model.Attribute;
import com.integration.em.model.Matchable;
import com.integration.em.similarity.LevenshteinSimilarity;
import com.integration.em.utils.ComparatorLogger;
import com.integration.em.model.Record;

public class RecordComparatorLevenshtein extends StringComparator {

	public RecordComparatorLevenshtein(Attribute attributeRecord1, Attribute attributeRecord2) {
		super(attributeRecord1, attributeRecord2);
	}

	private static final long serialVersionUID = 1L;
	private LevenshteinSimilarity sim = new LevenshteinSimilarity();
	private ComparatorLogger comparisonLog;

	@Override
	public double compare(Record record1, Record record2, Aligner<Attribute, Matchable> schemAlogner) {

		String s1 = record1.getValue(this.getAttributeRecord1());
		String s2 = record2.getValue(this.getAttributeRecord2());

		if (this.comparisonLog != null) {
			this.comparisonLog.setComparatorName(getClass().getName());

			this.comparisonLog.setRecord1Value(s1);
			this.comparisonLog.setRecord2Value(s2);
		}

		s1 = preprocess(s1);
		s2 = preprocess(s2);

		if (this.comparisonLog != null) {
			this.comparisonLog.setRecord1PreprocessedValue(s1);
			this.comparisonLog.setRecord2PreprocessedValue(s2);
		}

		// calculate similarity
		double similarity = sim.calculate(s1, s2);
		if (this.comparisonLog != null) {
			this.comparisonLog.setSimilarity(Double.toString(similarity));
		}

		return similarity;
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
