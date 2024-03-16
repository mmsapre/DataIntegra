package com.integration.em.parsers;


import com.integration.em.datatypes.DataType;
import com.integration.em.tables.ListHandler;
import com.integration.em.tables.Tbl;
import com.integration.em.tables.TblMapping;
import com.integration.em.tables.TblRow;
import com.integration.em.tables.lod.LodTblColumn;
import com.integration.em.tables.lod.LodTblRow;
import com.integration.em.utils.StringCache;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class LodCsvTableParser extends TableParser {


	public static String delimiter = "\",\"";
	
	private boolean useStringCache = true;
	private boolean parseLists = false;
	private boolean useRowIndexFromFile = true;
	

	public void setParseLists(boolean parseLists) {
		this.parseLists = parseLists;
	}
	

	public void setUseRowIndexFromFile(boolean useRowIndexFromFile) {
		this.useRowIndexFromFile = useRowIndexFromFile;
	}
	
	public void setUseStringCache(boolean use) {
		useStringCache = use;
	}
	
    public Tbl parseTable(File file) {
        Reader r = null;
        Tbl t = null;
        try {
            if (file.getName().endsWith(".gz")) {
                GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file));
                r = new InputStreamReader(gzip, "UTF-8");
            } else {
                r = new InputStreamReader(new FileInputStream(file), "UTF-8");
            }
            
            t = parseTable(r, file.getName());
            
            r.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return t;
    }
    
    public Tbl parseTable(Reader reader, String fileName) throws IOException {
        // create new table
        Tbl table = new Tbl();
        // take care of the header of the table
        table.setPath(fileName);

        // table may contain additional annotations (currently not used for DBpedia tables)
        TblMapping tm = new TblMapping();
        
        try {
            String[] columnNames;
            String[] columntypes;
            String[] columnURIs;
            String[] columnRanges;

            BufferedReader in = new BufferedReader(reader);

            String fileLine = in.readLine();
            columnNames = fileLine.split(delimiter);

            boolean isMetaData = columnNames[0].startsWith("#");
            while(isMetaData) {
                isMetaData = false;
                
                // check all valid annotations
                for(String s : TblMapping.VALID_ANNOTATIONS) {
                    if(columnNames[0].startsWith(s)) {
                        isMetaData = true;
                        break;
                    }
                }

                if(isMetaData) {
                	tm.parseMetadata(fileLine);
                	
                    fileLine = in.readLine();
                    columnNames = fileLine.split(delimiter);
                    isMetaData = columnNames[0].startsWith("#");
                }
            }

            columnURIs = in.readLine().split(delimiter);

            // read the datatypes
            fileLine = in.readLine();
            columntypes = fileLine.split(delimiter);

            // skip the last header (range)
            fileLine = in.readLine();
            columnRanges = fileLine.split(delimiter);
            
            // process all properties (=columns)
            int i = 0;
            for (String columnName : columnNames) {

                // replace trailing " for the last column
                columntypes[i] = columntypes[i].replace("\"", "");
                columnURIs[i] = columnURIs[i].replace("\"", "");
                columnRanges[i] = columnRanges[i].replace("\"", "");
                columnName = columnName.replace("\"", "");

                // create the column
                LodTblColumn c = new LodTblColumn(i, table);
                c.setHeader(columnName);
                
                if(columnName.endsWith("_label")) {
                	c.setReferenceLabel(true);
                }
                c.setUri(columnURIs[i]);
                c.setXmlType(columntypes[i]);
                c.setRange(columnRanges[i]);
                
                // set the type if it's a primitive
                //TODO what about other primitive types?
                String datatype = columntypes[i];
                switch (datatype) {
                case "XMLSchema#date":
                case "XMLSchema#gYear":
                    c.setDataType(DataType.date);
                    break;
                case "XMLSchema#double":
                case "XMLSchema#float":
                case "XMLSchema#nonNegativeInteger":
                case "XMLSchema#positiveInteger":
                case "XMLSchema#integer":    
                case "XMLSchema#negativeInteger": 
                case "minute":
                    c.setDataType(DataType.number);
                    break;
                case "XMLSchema#string":
                case "rdf-schema#Literal":
                    c.setDataType(DataType.string);                    
                    break;
                default:                    
                    c.setDataType(DataType.unknown);
                }

                // add the column to the table
                table.addColumn(c);
                i++;
            }
            
            int row = 4;
            if(!useRowIndexFromFile) {
            	row = 0;
            }
            Object[] values;
            
            // read the table rows
            while ((fileLine = in.readLine()) != null) {
                
            	// handle the column splitting
            	fileLine = fileLine.substring(1, fileLine.length() - 1);
                String[] stringValues = fileLine.split(delimiter);  
                
                // create the value array
                values = new Object[columnNames.length];
                
                // transfer values
            	for (int j = 0; j < stringValues.length; j++) {
					if(stringValues[j].equalsIgnoreCase("NULL")) {
						values[j] = null;
					} else {
						if(parseLists && ListHandler.checkIfList(stringValues[j])) {
							// value list
							String[] list = ListHandler.splitList(stringValues[j]);
							Object[] listValues = new Object[list.length];
							
							for(int listIndex = 0; listIndex < list.length; listIndex++) {
								if(useStringCache) {
									listValues[listIndex] = StringCache.get(list[listIndex]);
								} else {
									listValues[j] = list[listIndex];
								}
							}
							
							values[j] = listValues;
						} else {
							// single value
							if(useStringCache) {
								values[j] = StringCache.get(stringValues[j]);
							} else {
								values[j] = stringValues[j];
							}
						}
					}
				}
                

                TblRow r = new LodTblRow(row++, table);
                r.set(values);
                table.addRow(r);
            }
            
        } catch(Exception ex) {
        	ex.printStackTrace();
        }
        
        reader.close();
        
        if(isConvertValues()) {
        	table.inferSchemaAndConvertValues();
        }
        
        return table;
    }
	
    public static void endLoadData() {
    	StringCache.clear();
    }
}
