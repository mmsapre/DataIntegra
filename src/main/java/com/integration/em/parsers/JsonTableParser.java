package com.integration.em.parsers;

import com.google.gson.Gson;

import com.integration.em.datatypes.DataType;
import com.integration.em.detect.DetectType;
import com.integration.em.detect.SermiStructRowContentDetect;
import com.integration.em.tables.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class JsonTableParser extends TableParser {


	private boolean runGC = true;


	private boolean inferSchema = true;


	public JsonTableParser() {
		setDetectType(new TypeGuesser());
		setStringNormalizer(new DynaStringNormalizer());
		setRowContentDetect(new SermiStructRowContentDetect());
	}

	public JsonTableParser(DetectType pTypeDetector) {
		setDetectType(pTypeDetector);
		setStringNormalizer(new DynaStringNormalizer());
		setRowContentDetect(new SermiStructRowContentDetect());
	}

	public void setRunGC(boolean runGC) {
		this.runGC = runGC;
	}

	public boolean isInferSchema() {
		return inferSchema;
	}


	public void setInferSchema(boolean inferSchema) {
		this.inferSchema = inferSchema;
	}

	@Override
	public Tbl parseTable(File file) {
		FileReader fr;
		Tbl t = null;
		try {
			fr = new FileReader(file);

			t = parseTable(fr, file.getName());

			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return t;
	}

	@Override
	public Tbl parseTable(Reader reader, String fileName) throws IOException {
		Gson gson = new Gson();

		String json = IOUtils.toString(reader);

		// get the data from the JSON source
		JsonTableSchema data = gson.fromJson(json, JsonTableSchema.class);

		TblMapping mapping = null;

		if (data.getRelation() == null) {

			JsonTableWithMappingSchema moreData = gson.fromJson(json, JsonTableWithMappingSchema.class);

			data = moreData.getTable();
			mapping = moreData.getMapping().toTableMapping();
		}

		return parseTable(data, fileName, mapping);
	}

	public Tbl parseTable(JsonTableSchema data, String fileName, TblMapping mapping) {
		if (data.getTableType() != JsonTableSchema.TableType.RELATION) {
			return null;
		}

		if (data.getTableOrientation() == JsonTableSchema.TableOrientation.VERTICAL) {
			// flip table
			data.transposeRelation();
			data.setTableOrientation(JsonTableSchema.TableOrientation.HORIZONTAL);


			data.setHasHeader(true);
			data.setHeaderPosition(JsonTableSchema.HeaderPosition.FIRST_ROW);
			data.setHeaderRowIndex(0);
		}

		// create a new table
		Tbl t = new Tbl();
		t.setPath(fileName);
		t.setSubjectColumnIndex(data.getKeyColumnIndex());
		t.setTblId(data.getTableId());

		TblContext ctx = new TblContext();
		ctx.setTableNum(data.getTableNum());
		ctx.setUrl(data.getUrl());
		ctx.setPageTitle(data.getPageTitle());
		ctx.setTableTitle(data.getTitle());
		ctx.setTextBeforeTable(data.getTextBeforeTable());
		ctx.setTextAfterTable(data.getTextAfterTable());
		ctx.setTimestampBeforeTable(data.getTableContextTimeStampBeforeTable());
		ctx.setTimestampAfterTable(data.getTableContextTimeStampAfterTable());
		ctx.setLastModified(data.getLastModified());
		t.setTblContext(ctx);

		// detect header, if TableHeaderDetector is set
		int[] headerRowIndex;
		int[] emptyRowCount =	getRowContentDetect().detectEmptyHeaderRows(data.getRelation(), true);
		if (getTblHeaderDetect() != null) {
			headerRowIndex = getTblHeaderDetect().detectTableHeader(data.getRelation(), emptyRowCount);
			if (headerRowIndex != null)
				data.setHeaderRowIndex(headerRowIndex[0]);
			else
				data.setHasHeader(false);
		} else {
			headerRowIndex = new int[1];
			headerRowIndex[0] = data.getHeaderRowIndex();
			if(emptyRowCount != null && ArrayUtils.contains(emptyRowCount, headerRowIndex[0])){
				data.setHeaderRowIndex(emptyRowCount.length);
				headerRowIndex[0] = emptyRowCount.length;
			}
		}

		// create the table columns
		parseColumnData(data, t);

		parseDependencies(data, t);
		parseCandidateKeys(data, t);

		// create the rows, transpose the data first to convert from
		// column-based to row-based representation
		data.transposeRelation();
		
		//check for total row
		int[] sumRowCount	= 	getRowContentDetect().detectSumRow(data.getRelation());

		// populate table Content
		int[] skipRows = ArrayUtils.addAll(emptyRowCount, headerRowIndex);
		skipRows = ArrayUtils.addAll(skipRows, sumRowCount);
		populateTable(data.getRelation(), t, skipRows);
		if(runGC) System.gc();

		parseProvenance(data, t);

		if (isConvertValues()) {
			t.inferSchemaAndConvertValues(this.getDetectType());
		} else if (isInferSchema()) {
			t.inferSchema(this.getDetectType());
		}
		if(runGC) System.gc();

		if (mapping != null) {
			t.setTblMapping(mapping);
		}

		if (isInferSchema() && !t.hasSubjectColumn()) {
			t.identifySubjectColumn();
		}

		return t;
	}

	protected void parseProvenance(JsonTableSchema data, Tbl tbl) {
		if (data.getRowProvenance() != null) {
			for (int i = 0; i < data.getRowProvenance().length; i++) {
				String[] prov = data.getRowProvenance()[i];
				tbl.get(i).setProvenance(new ArrayList<>(Arrays.asList(prov)));
			}
		}
		if (data.getColumnProvenance() != null) {
			for (int i = 0; i < data.getColumnProvenance().length; i++) {
				String[] prov = data.getColumnProvenance()[i];
				tbl.getTblSchema().get(i).setProvenance(new ArrayList<>(Arrays.asList(prov)));
			}
		}
	}

	protected void parseCandidateKeys(JsonTableSchema data, Tbl tbl) {
		if (data.getCandidateKeys() != null && data.getCandidateKeys().length > 0) {
			Set<Set<TblColumn>> candidateKeys = new HashSet<>();
			for (Integer[] key : data.getCandidateKeys()) {
				Set<TblColumn> cols = new HashSet<>(key.length);
				for (Integer idx : key) {
					cols.add(tbl.getTblSchema().get(idx));
				}
				candidateKeys.add(cols);
			}
			tbl.getTblSchema().setCandidateKeys(candidateKeys);
		}
	}

	protected void parseDependencies(JsonTableSchema data, Tbl tbl) {
		if (data.getFunctionalDependencies() != null && data.getFunctionalDependencies().length > 0) {
			Map<Set<TblColumn>, Set<TblColumn>> dependencies = new HashMap<>();
			for (JsonTableSchema.Dependency fd : data.getFunctionalDependencies()) {
				if (fd.getDeterminant() != null) {
					Set<TblColumn> det = new HashSet<>();
					for (Integer i : fd.getDeterminant()) {
						det.add(tbl.getTblSchema().get(i));
					}
					Set<TblColumn> dep = new HashSet<>();
					for (Integer i : fd.getDependant()) {
						dep.add(tbl.getTblSchema().get(i));
					}
					dependencies.put(det, dep);
				}
			}
			tbl.getTblSchema().setFunctionalDependencies(dependencies);
		}
	}

	protected void parseColumnData(JsonTableSchema data, Tbl tbl) {
		String[] columnNames = data.getColumnHeaders();

		try {
			for (int colIdx = 0; colIdx < data.getRelation().length; colIdx++) {
				String columnName = null;

				if (columnNames != null && columnNames.length > colIdx) {
					columnName = columnNames[colIdx];
				} else {
					columnName = "";
				}

				TblColumn c = new TblColumn(colIdx, tbl);
				c.setDataType(DataType.unknown);

				// set the header
				String header = columnName;
				if (isCleanHeader()) {
					header = this.getStringNormalizer().normaliseHeader(columnName);
				}
				c.setHeader(header);

				tbl.addColumn(c);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
