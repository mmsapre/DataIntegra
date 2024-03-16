package com.integration.em.tables;


import com.integration.em.utils.MapUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.integration.em.datatypes.DataType;

public class TblNumberingExtract {

	private static final Pattern numberingPattern = Pattern.compile("(\\d*)\\. (.+)");

	public void removeNumbering(Collection<Tbl> tables) {
		for(Tbl t : tables) {
			// collect all numberings
			for(TblRow r : t.getTblRowArrayList()) {
				
				for(TblColumn c : t.getColumns()) {
					
					Object value = r.get(c.getColumnIndex());
					if(value!=null && !value.getClass().isArray()) { // ignore arrays for now
						
						String stringValue = value.toString();
						Matcher m = numberingPattern.matcher(stringValue);
						
						if(m.matches()) {
							
							String number = m.group(1);
							String rest = m.group(2).trim();

							// remove the numbering from the cell value
							r.set(c.getColumnIndex(), rest);
						}
						
					}
					
				}
				
			}
		}
	}

	public Map<Integer, Map<Integer, TblColumn>> extractNumbering(Collection<Tbl> tables
			) {

		// maps table id -> column id -> disambiguation column
		Map<Integer, Map<Integer, TblColumn>> tableToColumnToNumbering = new HashMap<>();
		
		for(Tbl t : tables) {
			
			Map<TblColumn, Map<TblRow, String>> numberings = new HashMap<>();
			
			// collect all numberings
			for(TblRow r : t.getTblRowArrayList()) {
				
				for(TblColumn c : t.getColumns()) {
					
					Object value = r.get(c.getColumnIndex());
					if(value!=null && !value.getClass().isArray()) { // ignore arrays for now
						
						String stringValue = value.toString();
						Matcher m = numberingPattern.matcher(stringValue);
						
						if(m.matches()) {
							
							String number = m.group(1);
							String rest = m.group(2).trim();
							
							Map<TblRow, String> innerMap = MapUtils.get(numberings, c, new HashMap<>());
							innerMap.put(r, number);
						
							// remove the numbering from the cell value
							r.set(c.getColumnIndex(), rest);
						}
						
					}
					
				}
				
			}
			
			Map<TblColumn, TblColumn> newColumns = new HashMap<>();
			
			// decide which new columns to create
			for(TblColumn c : numberings.keySet()) {
				Map<TblRow, String> values = numberings.get(c);
				
				double percent = values.size() / (double)t.getTblRowArrayList().size();
				
				if(percent>=0.05) {

					TblColumn newCol = new TblColumn(t.getColumns().size(), t);
					newCol.setDataType(DataType.number);
					
					if(c.getHeader()!=null && !"".equals(c.getHeader())) {
						newCol.setHeader(String.format("Context # of %s", c.getHeader()));
					}
					
					t.insertColumn(newCol.getColumnIndex(), newCol);
					
					newColumns.put(c, newCol);
					
					Map<Integer, TblColumn> columnToNumbering = MapUtils.get(tableToColumnToNumbering, t.getTblId(), new HashMap<>());
					columnToNumbering.put(c.getColumnIndex(), newCol);
				}
						
			}
			
			// fill new columns with values
			for(TblRow r : t.getTblRowArrayList()) {
				
				for(TblColumn c : numberings.keySet()) {

					TblColumn c2 = newColumns.get(c);
					
					if(c2!=null) {
					
						Map<TblRow, String> values = numberings.get(c);
						
						String value = values.get(r);
						
						if(value!=null) {
							
							r.set(c2.getColumnIndex(), value);
							
						}
					
					}
				}
				
			}
			
		}
		
		return tableToColumnToNumbering;
	}
}
