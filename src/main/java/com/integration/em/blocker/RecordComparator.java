package com.integration.em.blocker;

import com.integration.em.model.Record;

import com.integration.em.model.Attribute;
import com.integration.em.utils.Comparator;

public abstract class RecordComparator implements Comparator<Record, Attribute> {
	private Attribute attributeRecord1;
	private Attribute attributeRecord2;

	public RecordComparator(Attribute attributeRecord1, Attribute attributeRecord2) {
		super();
		this.setAttributeRecord1(attributeRecord1);
		this.setAttributeRecord2(attributeRecord2);
	}

	public Attribute getAttributeRecord1() {
		return attributeRecord1;
	}

	public void setAttributeRecord1(Attribute attributeRecord1) {
		this.attributeRecord1 = attributeRecord1;
	}

	public Attribute getAttributeRecord2() {
		return attributeRecord2;
	}

	public void setAttributeRecord2(Attribute attributeRecord2) {
		this.attributeRecord2 = attributeRecord2;
	}
}
