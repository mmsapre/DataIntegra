package com.integration.em.tables.app;

import au.com.bytecode.opencsv.CSVWriter;
import com.beust.jcommander.Parameter;

import com.integration.em.parsers.CsvTableParser;
import com.integration.em.parsers.JsonTableParser;
import com.integration.em.tables.*;
import com.integration.em.utils.Executable;
import com.integration.em.utils.Func;
import com.integration.em.utils.ProgressReporter;
import com.integration.em.utils.Q;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class ShowTableData extends Executable {

	@Parameter(names = "-d")
	private boolean showData = false;
	
	@Parameter(names = "-w")
	private int columnWidth = 20;
	
	@Parameter(names = "-keyColumnIndex")
	private Integer keyColumnIndex = null;
	
	@Parameter(names = "-convertValues")
	private boolean convertValues = false;
	
	@Parameter(names = "-update")
	private boolean update = false;
	
	@Parameter(names = "-detectKey")
	private boolean detectKey = false;
	
	@Parameter(names = "-listColumnIds")
	private boolean listColumnIds;
	
	@Parameter(names = "-header")
	private boolean showHeader = false;
	
	@Parameter(names = "-rows")
	private int numRows = 0;
	
	@Parameter(names = "-csv")
	private boolean createCSV = false;
	
	@Parameter(names = "-dep")
	private boolean showDependencyInfo = false;
	
	@Parameter(names = "-prov")
	private boolean showProvenanceInfo = false;
	
	@Parameter(names = "-pre")
	private boolean applyPreprocessing = false;
	
	public static void main(String[] args) throws IOException {
		ShowTableData s = new ShowTableData();
		
		if(s.parseCommandLine(ShowTableData.class, args) && s.getParams()!=null) {
			
			s.run();
		}
	}
	
	public void run() throws IOException {
		
		JsonTableParser p = new JsonTableParser();
		JsonTblWriter w = new JsonTblWriter();
		// p.setConvertValues(convertValues | detectKey);

		CsvTableParser csvP = new CsvTableParser();
		// csvP.setConvertValues(convertValues | detectKey);
		
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
		
		ProgressReporter prg = new ProgressReporter(files.length, "Processing Tables");
		
		CSVWriter csvW = null;
		if(createCSV) {
			csvW = new CSVWriter(new OutputStreamWriter(System.out));
		}
		
		for(String s : files) {
			
			Tbl t = null;
			
			File f = new File(s);
			if(dir!=null) {
				f = new File(dir,s);
			}
			
			try {
				if(s.endsWith("json")) {
					t = p.parseTable(f);
				} else if(s.endsWith("csv")) {
					t = csvP.parseTable(f);
				} else {
					log.error(String.format("Unknown table format '%s' (must be .json or .csv)", f.getName()));
					continue;
				}
				
				if(applyPreprocessing) {
					new TblAmbiguationExtract().extractDisambiguations(Q.toList(t));
					new TblNumberingExtract().extractNumbering(Q.toList(t));
				}
				
				if(convertValues) {
					t.convertValues();
				}

				// update the table if requested
				if(detectKey) {
					t.identifySubjectColumn(0.3,true);
					log.error(String.format("* Detected Entity-Label Column: %s", t.getSubjectColumn()==null ? "?" : t.getSubjectColumn().getHeader()));
				}
				if(keyColumnIndex!=null) {
					log.error(String.format("* Setting Entity-Label Column: %s", t.getTblSchema().get(keyColumnIndex)));
					t.setSubjectColumnIndex(keyColumnIndex);
				}
				if(update) {
					w.write(t, f);
				}
				
				if(createCSV) {
					// create a csv file with the table meta data
					csvW.writeNext(new String[] {
							s,
							Integer.toString(t.getTblRowArrayList().size()),
							Integer.toString(t.getColumns().size()),
							t.getTblContext()==null ? "" : t.getTblContext().getUrl(),
							t.getTblContext()==null ? "" : t.getTblContext().getPageTitle(),
							t.getTblContext()==null ? "" : t.getTblContext().getTableTitle(),
							Integer.toString(getOriginalTables(t).size()),
							t.getSubjectColumn()==null ? "" : Integer.toString(t.getSubjectColumn().getColumnIndex())
					});
				} else if(listColumnIds) {
					// list the columns in the table
					for(TblColumn c : t.getColumns()) {
						if(!showHeader) {
							System.out.println(c.getIdentifier());
						} else {
							System.out.println(c.toString());
						}
					}
				} else {
					// print the table meta data in human readable format

					TblContext ctx = t.getTblContext();
					
					System.out.println(String.format("*** Table %s ***", s));
					if(ctx!=null) {
						System.out.println(String.format("* URL: %s", ctx.getUrl()));
						System.out.println(String.format("* Title: %s", ctx.getPageTitle()));
						System.out.println(String.format("* Heading: %s", ctx.getTableTitle()));
					}
					System.out.println(String.format("* # Columns: %d", t.getColumns().size()));
					System.out.println(String.format("* # Rows: %d", t.getTblRowArrayList().size()));
					System.out.println(String.format("* Created from %d original tables", getOriginalTables(t).size()));
					System.out.println(String.format("* Entity-Label Column: %s", t.getSubjectColumn()==null ? "?" : t.getSubjectColumn().getHeader()));

					if(showProvenanceInfo) {
						// collect all provenance data
						Set<String> provenance = getOriginalTables(t);
						
						if(provenance.size()>0) {
							System.out.println("Provenance:");
							System.out.println(String.format("\t%s", 
									StringUtils.join(Q.sort(provenance), ",")
									));
						} else {
							System.out.println("Table has no provenance data attached.");
						}
					}
					
					if(showDependencyInfo) {
						
						if(t.getTblSchema().getFunctionalDependencies()!=null && t.getTblSchema().getFunctionalDependencies().size()>0) {
							System.out.println("*** Functional Dependencies ***");
							for(Collection<TblColumn> det : t.getTblSchema().getFunctionalDependencies().keySet()) {
								Collection<TblColumn> dep = t.getTblSchema().getFunctionalDependencies().get(det);
								System.out.println(
										String.format(
												"{%s}->{%s}", 
												StringUtils.join(Q.project(det, new TblColumn.ColumnHeaderProjection()), ","),
												StringUtils.join(Q.project(dep, new TblColumn.ColumnHeaderProjection()), ",")));
							}
						}
						if(t.getTblSchema().getCandidateKeys()!=null && t.getTblSchema().getCandidateKeys().size()>0) {
							System.out.println("*** Candidate Keys ***");
							for(Collection<TblColumn> candidateKey : t.getTblSchema().getCandidateKeys()) {
								System.out.println(
										String.format("{%s}", StringUtils.join(Q.project(candidateKey, new TblColumn.ColumnHeaderProjection()), ",")));
							}
						}
					}

					if(showData) {
						System.out.println(t.getTblSchema().format(columnWidth));
						System.out.println(t.getTblSchema().formatDataTypes(columnWidth));
						
						int maxRows = Math.min(numRows, t.getTblRowArrayList().size());
						
						if(maxRows==0) {
							maxRows = t.getTblRowArrayList().size();
						}
						
						for(int i = 0; i < maxRows; i++) {
							TblRow r = t.getTblRowArrayList().get(i);
							if(showProvenanceInfo) {
								System.out.println(StringUtils.join(r.getProvenance(), " / "));
							}
							System.out.println(r.format(columnWidth));
						}
					} else {
						System.out.println(StringUtils.join(Q.project(t.getColumns(), 
								new Func<String, TblColumn>() {
		
									@Override
									public String invoke(TblColumn in) {
										return String.format("%s (%s)", in.getHeader(), in.getDataType());
									}}
								), ", "));
					}
				
					prg.incrementProgress();
					prg.report();
				}
			} catch(Exception e) {
				System.err.println(String.format("Cannot process table '%s'!",f));
				e.printStackTrace();
			}
		}
		
		if(createCSV) {
			csvW.close();
		}
		
	}
	
	private Set<String> getOriginalTables(Tbl t) {
		
		Set<String> tbls = new HashSet<>();
		
		for(TblColumn c : t.getColumns()) {
			for(String prov : c.getProvenance()) {
				
				tbls.add(prov.split("~")[0]);
				
			}
		}
		
		return tbls;
	}
}
