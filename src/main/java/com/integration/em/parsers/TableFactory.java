
package com.integration.em.parsers;

import com.integration.em.tables.Tbl;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class TableFactory {
	

	private Map<String, TableParser> parsers = new HashMap<>();
	
	public void addParser(String extension, TableParser p) {
		parsers.put(extension, p);
	}
	
	public TableFactory() {
		addParser(".json", new JsonTableParser());
		addParser(".csv", new CsvTableParser());
		addParser(".csv.gz", new CsvTableParser());
	}
	
	public Tbl createTableFromFile(File f) {
		Tbl t = null;
		TableParser p = null;
		
		for(String extension : parsers.keySet()) {
			if(f.getName().endsWith(extension)) {
				p = parsers.get(extension);
			}
		}
		
		if(p!=null) {
			p.setConvertValues(false);
			t = p.parseTable(f);
		} else {
			log.error(String.format("Unsupported table format: %s", f.getName()));
		}
		
		return t;
	}
	
}
