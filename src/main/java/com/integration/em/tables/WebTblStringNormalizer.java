package com.integration.em.tables;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class WebTblStringNormalizer {

	public static String nullValue = "NULL";
	public static List<String> stopWords = new ArrayList<>();
	private static final List<String> possibleNullValues = new ArrayList<String>() {

		private static final long serialVersionUID = 1L;

	{ 
		add(""); 
		add("__"); 
		add("-"); 
		add("_"); 
		add("?"); 
		add("unknown"); 
		add("- -"); 
		add("n/a"); 
		add("•"); 
		add("- - -"); 
		add("."); 
		add("??"); 
		add("(n/a)"); 
		}};

	/**
	 *
	 * @param columnName
	 * @return the normalised string
	 */
	public static String normaliseHeader(String columnName) {
		if(columnName==null)
		{
			return "";
		} else {
			columnName = StringEscapeUtils.unescapeJava(columnName);
			columnName = columnName.replace("\"", "");
			columnName = columnName.replace("|", " ");
			columnName = columnName.replace(",", "");
			columnName = columnName.replace("{", "");
			columnName = columnName.replace("}", "");
			columnName = columnName.replaceAll("\n", "");

			columnName = columnName.replace("&nbsp;", " ");
			columnName = columnName.replace("&nbsp", " ");
			columnName = columnName.replace("nbsp", " ");
			columnName = columnName.replaceAll("<.*>", "");

			columnName = columnName.toLowerCase();
			columnName = columnName.trim();

			columnName = columnName.replaceAll("\\.", "");
			columnName = columnName.replaceAll("\\$", "");
			// clean the values from additional strings
			// if (columnName.contains("/")) {
			// 	columnName = columnName.substring(0, columnName.indexOf("/"));
			// }

			// if (columnName.contains("\\")) {
			// 	columnName = columnName.substring(0, columnName.indexOf("\\"));
			// }
			if (possibleNullValues.contains(columnName)) {
				columnName = nullValue;
			}
			
			return columnName;
		}
	}

	private static final Pattern bracketsPattern = Pattern.compile("\\(.*\\)");

	public static String normaliseValue(String value, boolean removeContentInBrackets) {
		try {
			value = value.replaceAll("\n", "");
			value = value.replace("&nbsp;", " ");
			value = value.replace("&nbsp", " ");
			value = value.replaceAll("[&\\?]#[0-9]{1,3};", "");
			value = value.replace("nbsp", " ");
			value = value.replaceAll("<.*>", "");
			value = value.toLowerCase();
			value = value.trim();
			if (possibleNullValues.contains(value)) {
				value = nullValue;
			}
			if (removeContentInBrackets) {
				value = bracketsPattern.matcher(value).replaceAll("");
			}
		} catch (Exception e) {
		}
		return value;
	}


	public static String normalise(String s, boolean useStemmer) {

		return StringUtils.join(tokenise(s, useStemmer), " ");

	}

	public static List<String> tokenise(String s, boolean useStemmer) {
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		List<String> result = new ArrayList<String>();

		try {

			Map<String, String> args = new HashMap<String, String>();
			args.put("generateWordParts", "1");
			args.put("generateNumberParts", "1");
			args.put("catenateNumbers", "0");
			args.put("splitOnCaseChange", "1");
			WordDelimiterFilterFactory fact = new WordDelimiterFilterFactory(args);

			// resolve non unicode chars
			s = StringEscapeUtils.unescapeJava(s);

			// remove brackets (but keep content)
			s = s.replaceAll("[\\(\\)]", "");

			// tokenise
			TokenStream stream = fact.create(new WhitespaceTokenizer(Version.LUCENE_46, new StringReader(s)));
			stream.reset();

			if (useStemmer) {
				// use stemmer if requested
				stream = new PorterStemFilter(stream);
			}

			// lower case all tokens
			stream = new LowerCaseFilter(Version.LUCENE_46, stream);

			// remove stop words
			stream = new StopFilter(Version.LUCENE_46, stream, ((StopwordAnalyzerBase) analyzer).getStopwordSet());

			// enumerate tokens
			while (stream.incrementToken()) {
				result.add(stream.getAttribute(CharTermAttribute.class).toString());
			}
			stream.close();
		} catch (IOException e) {
			// not thrown b/c we're using a string reader...
		}

		analyzer.close();

		return result;
	}
}
