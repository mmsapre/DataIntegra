
package com.integration.em.tables.lod;

import com.integration.em.tables.Tbl;
import com.integration.em.tables.TblRow;

public class LodTblRow extends TblRow {

	private static final long serialVersionUID = 1L;

	public LodTblRow(int rowNumber, Tbl table) {
		super(rowNumber, table);
	}

	@Override
	public String getIdentifier() {
		return getValueArray()[0].toString();
	}
}
