package com.integration.em.tables.app;

import au.com.bytecode.opencsv.CSVWriter;
import com.beust.jcommander.Parameter;
import com.integration.em.parsers.CsvTableParser;
import com.integration.em.parsers.JsonTableParser;
import com.integration.em.tables.*;
import com.integration.em.utils.Executable;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
@Slf4j
public class FeatureGenerator extends Executable {

	@Parameter(names = "-web")
	private String webTablesLocation;
	
	@Parameter(names = "-list")
	private boolean calculateList;
	
	@Parameter(names = "-horizontallyStacked")
	private boolean calculateHorizontallyStacked;
	
	@Parameter(names = "-rowNumbers")
	private String rowNumbersFile;
	

	public static void main(String[] args) throws IOException {
		FeatureGenerator g = new FeatureGenerator();
		
		if(g.parseCommandLine(FeatureGenerator.class, args)) {
			g.run();
		}
	}
	
	public void run() throws IOException {
		File source = new File(webTablesLocation);
		int done = 0;
		
		if(source.exists()) {

			LinkedList<String> names = new LinkedList<>();
			LinkedList<Feature> features = new LinkedList<>();
			
			names.add("TableName");
			
			if(calculateList) {
				names.add("List");
				features.add(new ListFeature());
			}
			
			if(calculateHorizontallyStacked) {
				names.add("HorizontallyStacked");
				features.add(new HorizontallyStackedFeature());
			}
			
			CsvTableParser csvParser = new CsvTableParser();
			JsonTableParser jsonParser = new JsonTableParser();

			CSVWriter w = new CSVWriter(new OutputStreamWriter(System.out));
			String[] header = new String[names.size()];
			names.toArray(header);
			w.writeNext(header);
			
			File[] tableFiles = null;
			
			if(source.isDirectory()) {
				tableFiles = source.listFiles();
			} else {
				tableFiles = new File[] { source };
			}
			
			CSVWriter wRow = null;
			
			if(rowNumbersFile!=null) {
				wRow = new CSVWriter(new FileWriter(new File(rowNumbersFile)));
				wRow.writeNext(new String[] { "TableName", "InstanceUri", "RowNumber" });
			}
			
			int progressStep = Math.max(10000, tableFiles.length/100);
			
			for(File tableFile : tableFiles) {
				Tbl t = null;
				
				try {
					if(tableFile.getName().endsWith("csv")) {
						t = csvParser.parseTable(tableFile);
					} else if(tableFile.getName().endsWith("json")) {
						t = jsonParser.parseTable(tableFile);
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
				
				if(t==null) {
					log.error(String.format("Unknown input format: %s", tableFile.getName()));
					continue;
				}
				
				String[] values = new String[names.size()];
				values[0] = tableFile.getName();
				boolean anyValue = false;
				for(int i = 0; i < features.size(); i++) {
					double value = features.get(i).calculate(t);
					if(value!=0.0) {
						anyValue=true;
					}
					values[i+1] = Double.toString(value);
				}
				
				if(anyValue) {
					w.writeNext(values);
				}
				
				/*
				 * Process Table Mapping (CSV only) 
				 */
				
				if(tableFile.getName().endsWith("csv") && wRow!=null) {
					TblMapping tm = TblMapping.read(tableFile.getAbsolutePath());
					for(int row = 0; row < tm.getMappedInstances().length; row++) {
						if(tm.getMappedInstance(row)!=null) {
							wRow.writeNext(new String[] { tableFile.getName(), tm.getMappedInstance(row).getFirst(), Integer.toString(row)});
						}
					}
				}
				
				if(done%progressStep==0) {
					log.error(String.format("%2.2f%%: %s", (float)done/(float)tableFiles.length*100.0, tableFile.getName()));
					w.flush();
				}
				done++;
			}
			
			w.close();
			
			if(wRow!=null) {
				wRow.close();
			}
		} else {
			log.error("Could not find web tables!");
		}
	}
}
