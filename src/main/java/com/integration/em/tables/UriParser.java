package com.integration.em.tables;

import com.google.gson.Gson;

import com.integration.em.tables.JsonTblUri;
import com.integration.em.tables.JsonTblWithMapping;
import com.integration.em.tables.Tbl;
import com.integration.em.tables.TblContext;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class UriParser {
	

    public Tbl parseTbl(File file) {
        FileReader fr;
        Tbl t = null;
        try {
            fr = new FileReader(file);
            
            t = parseTbl(fr, file.getName());
            
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return t;
    }
    
    public Tbl parseTbl(Reader reader, String fileName) throws IOException {
        Gson gson = new Gson();
        
        String json = IOUtils.toString(reader);
        
        // get the data from the JSON source
        JsonTblUri data = gson.fromJson(json, JsonTblUri.class);

        if(data.getUrl()==null) {
        	JsonTblWithMapping dataWithMapping = gson.fromJson(json, JsonTblWithMapping.class);
        	if(dataWithMapping.getJsonTblUri()!=null) {
        		data = dataWithMapping.getJsonTblUri();
        	}
        }
        
        if(data.getUrl()!=null) {
        	Tbl t = new Tbl();
        	t.setPath(fileName);
        	TblContext c = new TblContext();
        	c.setUrl(data.getUrl());
        	t.setTblContext(c);
        	return t;
        } else {
        	return null;
        }
        
    }
}
