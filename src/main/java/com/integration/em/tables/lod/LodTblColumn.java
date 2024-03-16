package com.integration.em.tables.lod;


import com.integration.em.tables.Tbl;
import com.integration.em.tables.TblColumn;

public class LodTblColumn extends TblColumn {


	private String xmlType;
	private String range;
	
	private boolean isReferenceLabel = false;
	
	public void setReferenceLabel(boolean isReferenceLabel) {
		this.isReferenceLabel = isReferenceLabel;
	}
	
	public boolean isReferenceLabel() {
		return isReferenceLabel;
	}

	public LodTblColumn(int columnIndex, Tbl table) {
		super(columnIndex, table);
	}

	@Override
	public String getIdentifier() {
		return getUri();
	}
	
	@Override
	public String getUniqueName() {
		return getTable().getPath() + "/" + super.getUniqueName() + "/" + isReferenceLabel;
	}
	

	public String getXmlType() {
		return xmlType;
	}

	public void setXmlType(String xmlType) {
		this.xmlType = xmlType;
	}

	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}
}
