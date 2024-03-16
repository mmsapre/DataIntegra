
package com.integration.em.blocker;

import com.integration.em.model.Aligner;
import com.integration.em.model.Attribute;
import com.integration.em.model.Matchable;
import com.integration.em.rules.compare.IComparator;
import com.integration.em.rules.compare.IComparatorLogger;
import com.integration.em.similarity.TokenizingJaccardSimilarity;
import com.integration.em.utils.Comparator;
import com.integration.em.utils.ComparatorLogger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LabelComparatorJaccard implements IComparator<Attribute, Attribute> {

	private TokenizingJaccardSimilarity similarity = new TokenizingJaccardSimilarity();
	private IComparatorLogger comparisonLog;

	@Override
	public double compare(Attribute record1, Attribute record2, Aligner<Attribute, Matchable> schemaAligner) {
		double sim = similarity.calculate(record1.getName(), record2.getName());

		log.info("Compare record 1 :"+record1.getName());
		log.info("Compare record 2 :"+record2.getName());
		log.info("Similar :"+sim);

		if (this.comparisonLog != null) {
			this.comparisonLog.setComparatorName(getClass().getName());

			this.comparisonLog.setRecord1Value(record1.getName());
			this.comparisonLog.setRecord2Value(record2.getName());

			this.comparisonLog.setSimilarity(Double.toString(sim));
		}

		return sim;
	}

	@Override
	public IComparatorLogger getComparisonLog() {
		return this.comparisonLog;
	}

	@Override
	public void setComparisonLog(IComparatorLogger comparatorLog) {
		this.comparisonLog = comparatorLog;
	}

}
