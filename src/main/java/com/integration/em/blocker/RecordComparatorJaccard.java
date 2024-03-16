
package com.integration.em.blocker;


import com.integration.em.model.Aligner;
import com.integration.em.model.Attribute;
import com.integration.em.model.Matchable;
import com.integration.em.similarity.TokenizingJaccardSimilarity;
import com.integration.em.utils.ComparatorLogger;
import com.integration.em.model.Record;

public class RecordComparatorJaccard extends StringComparator {

	private static final long serialVersionUID = 1L;
	TokenizingJaccardSimilarity sim = new TokenizingJaccardSimilarity();

	private ComparatorLogger comparisonLog;

	public RecordComparatorJaccard(Attribute attributeRecord1, Attribute attributeRecord2, double threshold,
								   boolean squared) {
		super(attributeRecord1, attributeRecord2);
		this.threshold = threshold;
		this.squared = squared;
	}

	private double threshold;
	private boolean squared;

	@Override
	public double compare(Record record1, Record record2, Aligner<Attribute, Matchable> schemaCorrespondence) {

		// preprocessing
		String s1 = record1.getValue(this.getAttributeRecord1());
		String s2 = record2.getValue(this.getAttributeRecord2());

		if (this.comparisonLog != null) {
			this.comparisonLog.setComparatorName(getClass().getName());

			this.comparisonLog.setRecord1Value(s1);
			this.comparisonLog.setRecord2Value(s2);
		}

		if (s1 == null || s2 == null) {
			return 0.0;
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

		// postprocessing
		if (similarity <= this.threshold) {
			similarity = 0;
		}
		if (squared)
			similarity *= similarity;

		if (this.comparisonLog != null) {
			this.comparisonLog.setPostprocessedSimilarity(Double.toString(similarity));
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
