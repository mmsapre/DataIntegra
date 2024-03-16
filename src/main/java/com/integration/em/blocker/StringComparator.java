package com.integration.em.blocker;

import com.integration.em.model.Attribute;
import com.integration.em.model.Record;

public abstract class StringComparator extends RecordComparator {

	public StringComparator(Attribute attributeRecord1, Attribute attributeRecord2) {
		super(attributeRecord1, attributeRecord2);
	}

	private static final long serialVersionUID = 1L;
	private boolean removeBrackets = false;
	private boolean lowerCase = false;
	
	
	
	public boolean isRemoveBrackets() {
		return removeBrackets;
	}

	public boolean isLowerCase() {
		return lowerCase;
	}

	public void setRemoveBrackets(boolean removeBrackets) {
		this.removeBrackets = removeBrackets;
	}
	public void setLowerCase(boolean lowerCase) {
		this.lowerCase = lowerCase;
	}

	protected String preprocess(String s) {
		
		if(s==null) {
			return null;
		} else {
			
			if(removeBrackets) {
				s = s.replaceAll("\\(.*\\)", "");
			}
			
			if(lowerCase) {
				s = s.toLowerCase();
			}
			
			return s;
		}
	}

}
