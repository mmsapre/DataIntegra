package com.integration.em.Tbls.app;

import com.beust.jcommander.Parameter;

import com.integration.em.parsers.JsonTableParser;
import com.integration.em.parsers.JsonTableParser;
import com.integration.em.tables.CSVTblWriter;
import com.integration.em.tables.Tbl;
import com.integration.em.tables.TblColumn;
import com.integration.em.tables.TblRow;
import com.integration.em.utils.Executable;
import com.integration.em.utils.FileUtils;
import com.integration.em.utils.TblStringUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
@Slf4j
public class JsonToCsvConverter extends Executable {

	@Parameter(names = "-json", required=true)
	private String jsonLocation;
	
	@Parameter(names = "-result", required=true)
	private String resultLocation;

	@Parameter(names = "-addRowProvenance")
	private boolean addRowProcenance;
	
	public static void main(String[] args) throws Exception {
		JsonToCsvConverter conv = new JsonToCsvConverter();
		
		if(conv.parseCommandLine(JsonToCsvConverter.class, args)) {
			
			conv.run();
			
		}
	}
	
	public void run() throws Exception {
		
		File jsonFile = new File(jsonLocation);
		File resultFile = new File(resultLocation);
		
		if(resultFile.exists()) {
			 resultFile.mkdirs();
		}
		
		JsonTableParser p = new JsonTableParser();
		p.setConvertValues(false);
		
		CSVTblWriter w = new CSVTblWriter();
		
		for(File f : FileUtils.listAllFiles(jsonFile)) {
			log.info(String.format("Converting %s", f.getName()));
			
			Tbl t = p.parseTable(f);
			
			if(addRowProcenance) {
				TblColumn prov = new TblColumn(t.getColumns().size(), t);
				prov.setHeader("Row provenance");
				t.insertColumn(prov.getColumnIndex(), prov);
				for(TblRow r : t.getTblRowArrayList()) {
					r.set(prov.getColumnIndex(), TblStringUtils.join(r.getProvenance(), " "));
				}
			}

			w.write(t, new File(resultFile, t.getPath()));
		}
		
	}
	
}
