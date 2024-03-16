
package com.integration.em.tables.app;

import com.beust.jcommander.Parameter;
import com.integration.em.parsers.CsvTableParser;
import com.integration.em.parsers.JsonTableParser;
import com.integration.em.tables.CSVTblWriter;
import com.integration.em.tables.JsonTblWriter;
import com.integration.em.tables.Tbl;
import com.integration.em.tables.TblWriter;
import com.integration.em.utils.Executable;
import com.integration.em.utils.ProgressReporter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.File;

@Slf4j
public class ConvertTable extends Executable {

	public static enum format
	{
		CSV,
		JSON,

	}
	
	@Parameter(names = "-format", required=true)
	private format outputFormat;
	
	@Parameter(names = "-out", required=true)
	private String outputDirectory;
	

	public static void main(String[] args) throws Exception {
		ConvertTable ct = new ConvertTable();
		
		if(ct.parseCommandLine(ConvertTable.class, args)) {
			ct.run();
		}
	}
	
	public void run() throws Exception {
	
		String[] files = getParams().toArray(new String[getParams().size()]);
		
		File dir = null;
		if(files.length==1) {
			dir = new File(files[0]);
			if(dir.isDirectory()) {
				files = dir.list();
			} else {
				dir = null;
			}
		}
		
		CsvTableParser csvParser = new CsvTableParser();
		JsonTableParser jsonParser = new JsonTableParser();
		
		TblWriter writer;
		
		switch (outputFormat) {
		case CSV:
			writer = new CSVTblWriter();
			break;
		case JSON:
			writer = new JsonTblWriter();
			break;

		default:
			log.error("Invalid output format specified!");
			return;
		}
		
		File outDir = new File(outputDirectory);
		outDir.mkdirs();
		
		ProgressReporter p = new ProgressReporter(files.length, "Converting Files");
		
		for(String file : files) {
			
			Tbl t = null;
			
			File f = new File(file);
			if(dir!=null) {
				f = new File(dir,file);
			}
			
			if(file.endsWith("csv")) {
				t = csvParser.parseTable(f);
			} else if(file.endsWith("json")) {
				t = jsonParser.parseTable(f);
			} else {
				log.error(String.format("Cannot parse table '%s' (file format must be 'csv' or 'json')!", file));
			}
			
			if(t!=null) {
				writer.write(t, new File(outDir, file));
			}
			
			p.incrementProgress();
			p.report();
		}
		
	}
	
}
