package com.integration.em.parsers;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class JsonTableWithMappingSchema {

    private JsonTableSchema table;
    private JsonTableMapping mapping;
    
    public JsonTableSchema getTable() {
        return table;
    }
    public void setTable(JsonTableSchema table) {
        this.table = table;
    }
    
    public JsonTableMapping getMapping() {
        return mapping;
    }
    public void setMapping(JsonTableMapping mapping) {
        this.mapping = mapping;
    }
    
    public void writeJson(File file) throws IOException {
        Gson gson = new Gson();
        BufferedWriter w = new BufferedWriter(new FileWriter(file));
        w.write(gson.toJson(this));
        w.close();
    }
    
    public static JsonTableWithMappingSchema fromJson(File file) throws JsonSyntaxException, IOException {
        Gson gson = new Gson();
        return gson.fromJson(FileUtils.readFileToString(file), JsonTableWithMappingSchema.class);
    }
	
}
