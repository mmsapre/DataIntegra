package com.integration.em.tables;


import com.integration.em.datatypes.DataType;
import com.integration.em.tables.Feature;
import com.integration.em.tables.Tbl;
import com.integration.em.tables.TblColumn;
import com.integration.em.tables.WebTblStringNormalizer;

import java.util.HashMap;
import java.util.LinkedList;


public class HorizontallyStackedFeature implements Feature {


	@Override
	public double calculate(Tbl t) {
		
		LinkedList<String> columns = new LinkedList<>();
		HashMap<String, Integer> counts = new HashMap<>();
		
		for(TblColumn tc : t.getColumns()) {
			if(
					tc.getHeader()!=null 
					&& !tc.getHeader().trim().isEmpty() 
					&& !tc.getHeader().equalsIgnoreCase(WebTblStringNormalizer.nullValue)) {
				String col = String.format("%s%s", tc.getHeader(), tc.getDataType()== DataType.string?"string":"nonstring");
				columns.add(col);
				Integer c = counts.get(col);
				if(c==null) {
					c = 0;
				}
				counts.put(col, c+1);
			}
		}
		
		// check if all counts are the same
		int count = -1;
		for(Integer c : counts.values()) {
			if(count==-1) {
				count = c;
			} else {
				if(count!=c) {
					return 0.0;
				}
			}
		}
		
		// count now tells us how often the Tbl is stacked
		// so in the list of columns without empty columns, we know where the column must be repeated
		if(count<2) {
			return 0.0;
		}
		
		int numAttributes = counts.size();
		for(int index=0; index < numAttributes; index++) {
			String col = columns.get(index);
			
			for(int i = 1; i < count; i++) {
				String next = columns.get(index + i * numAttributes);
				if(!col.equals(next)) {
					return 0.0;
				}
			}
		}

		return 1.0;
	}

}
