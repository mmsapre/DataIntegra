
package com.integration.em.blocker;


import com.integration.em.model.Aligner;
import com.integration.em.model.Attribute;
import com.integration.em.model.Matchable;
import com.integration.em.similarity.OverlapSimilarity;
import com.integration.em.utils.Comparator;
import com.integration.em.utils.ComparatorLogger;

import java.util.ArrayList;
import java.util.List;
import com.integration.em.model.Record;

public class RecordComparatorOverlapMultipleAttributes implements Comparator<Record, Attribute> {

	private static final long serialVersionUID = 1L;

	private OverlapSimilarity similarity = new OverlapSimilarity();
	private List<Attribute> attributeRecords1;
	private List<Attribute> attributeRecords2;

	private ComparatorLogger comparisonLog;

	public RecordComparatorOverlapMultipleAttributes(List<Attribute> attributeRecords1,
			List<Attribute> attributeRecords2) {
		super();
		this.setAttributeRecords1(attributeRecords1);
		this.setAttributeRecords2(attributeRecords2);
	}

	@Override
	public double compare(Record record1, Record record2, Aligner<Attribute, Matchable> schemaAligner) {

		ArrayList<String> first = new ArrayList<String>();
		ArrayList<String> second = new ArrayList<String>();

		for (Attribute firstAttribute : this.attributeRecords1) {
			String valuesTemp = record1.getValue(firstAttribute);
			if (valuesTemp != null) {
				String valuesArray[] = valuesTemp.split(" ");
				for (String value : valuesArray) {
					first.add(value.toLowerCase());
				}
			}
		}

		for (Attribute secondAttribute : this.attributeRecords2) {
			String valuesTemp = record2.getValue(secondAttribute);
			if (valuesTemp != null) {
				String valuesArray[] = valuesTemp.split(" ");
				for (String value : valuesArray) {
					second.add(value.toLowerCase());
				}
			}
		}

		if (!first.isEmpty() && !second.isEmpty()) {
			double sim = similarity.calculate(first, second);
			if (this.comparisonLog != null) {
				this.comparisonLog.setComparatorName(getClass().getName());

				this.comparisonLog.setRecord1Value(first.toString());
				this.comparisonLog.setRecord2Value(second.toString());

				this.comparisonLog.setSimilarity(Double.toString(sim));
			}

			return sim;
		}

		return 0;
	}

	public List<Attribute> getAttributeRecords1() {
		return attributeRecords1;
	}

	public void setAttributeRecords1(List<Attribute> attributeRecords1) {
		this.attributeRecords1 = attributeRecords1;
	}

	public List<Attribute> getAttributeRecords2() {
		return attributeRecords2;
	}

	public void setAttributeRecords2(List<Attribute> attributeRecords2) {
		this.attributeRecords2 = attributeRecords2;
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
