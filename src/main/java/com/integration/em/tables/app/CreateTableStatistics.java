package com.integration.em.tables.app;

import au.com.bytecode.opencsv.CSVWriter;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.integration.em.parsers.JsonTableSchema;
import com.integration.em.parsers.JsonTableWithMappingSchema;
import com.integration.em.utils.Executable;
import org.apache.commons.io.IOUtils;

import java.io.*;


public class CreateTableStatistics extends Executable {

	@Parameter(names = "-tables", required = true)
	private String tablesLocation;
	
	@Parameter(names = "-results", required = true)
	private String resultsLocation;
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		CreateTableStatistics exe = new CreateTableStatistics();
		
		if(exe.parseCommandLine(CreateTableStatistics.class, args)) {
		
			exe.run();
			
		}
		
	}
	
	public void run() throws FileNotFoundException, IOException {
		
		CSVWriter resultStatisticsWriter = new CSVWriter(new FileWriter(new File(new File(resultsLocation), "table_statistics.csv"), true));
		
		for(File f : new File(tablesLocation).listFiles()) {
	        Gson gson = new Gson();
	        
	        String json = IOUtils.toString(new FileInputStream(f));
	        
	        // get the data from the JSON source
	        JsonTableSchema data = gson.fromJson(json, JsonTableSchema.class);

	        if(data.getRelation()==null) {
	        	
	        	JsonTableWithMappingSchema moreData = gson.fromJson(json, JsonTableWithMappingSchema.class);
	        	
	        	data = moreData.getTable();
	        }
	        
	        if(data.getRelation()!=null) {
	        	
	        	int rows = 0;
	        	int cols = data.getRelation().length;
	        	
	        	for(String[] values : data.getRelation()) {
	        		rows = Math.max(values.length, rows);
	        	}
	        	
	        	rows -= data.getNumberOfHeaderRows();
	        	
    			resultStatisticsWriter.writeNext(new String[] {
    					new File(tablesLocation).getName(),
    					f.getName(),
    					Integer.toString(rows),
    					Integer.toString(cols)
    			});
	    		
	        }
		}
		
		resultStatisticsWriter.close();
	
		
	}
	
}
