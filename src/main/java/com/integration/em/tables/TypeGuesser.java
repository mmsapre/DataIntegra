package com.integration.em.tables;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.integration.em.datatypes.*;
import com.integration.em.detect.DetectType;
import com.integration.em.parsers.UnitParser;
import com.integration.em.utils.MapUtils;
import com.integration.em.utils.Q;

public class TypeGuesser implements DetectType {

	private static Pattern listCharactersPattern = Pattern.compile("\\{|\\}");

	public ColumnType guessTypeForValue(String columnValue, Unit headerUnit) {
		if (checkIfList(columnValue)) {
			List<String> columnValues;
			// columnValue = columnValue.replace("{", "");
			// columnValue = columnValue.replace("}", "");
			columnValue = listCharactersPattern.matcher(columnValue).replaceAll("");
			columnValues = Arrays.asList(columnValue.split("\\|"));
			Map<DataType, Integer> countTypes = new HashMap<>();
			Map<Unit, Integer> countUnits = new HashMap<>();
			for (String singleValue : columnValues) {
				ColumnType guessedSingleType = guessTypeForSingleValue(singleValue, headerUnit);

				Integer cnt = countTypes.get(guessedSingleType.getType());
				if (cnt == null) {
					cnt = 0;
				}
				countTypes.put(guessedSingleType.getType(), cnt + 1);


				cnt = countUnits.get(guessedSingleType.getUnit());
				if (cnt == null) {
					cnt = 0;
				}
				countUnits.put(guessedSingleType.getUnit(), cnt + 1);

			}
			int max = 0;
			DataType finalType = null;
			for (DataType type : countTypes.keySet()) {
				if (countTypes.get(type) > max) {
					max = countTypes.get(type);
					finalType = type;
				}
			}
			max = 0;
			Unit finalUnit = null;
			for (Unit type : countUnits.keySet()) {
				if (countUnits.get(type) > max) {
					max = countUnits.get(type);
					finalUnit = type;
				}
			}
			return new ColumnType(finalType, finalUnit);
		} else {
			return guessTypeForSingleValue(columnValue, headerUnit);
		}
	}

	private static Pattern listPattern = Pattern.compile("^\\{.+\\|.+\\}$");

	private boolean checkIfList(String columnValue) {
		// if (columnValue.matches("^\\{.+\\|.+\\}$")) {
		if (columnValue != null && listPattern.matcher(columnValue).matches()) {
			return true;
		}
		return false;
	}

	private ColumnType guessTypeForSingleValue(String columnValue, Unit headerUnit) {
		if (columnValue != null) {
			// check the length
			boolean validLenght = true;
			if (columnValue.length() > 50) {
				validLenght = false;
			}
			if (validLenght && Boolean.parseBoolean(columnValue)) {
				return new ColumnType(DataType.bool, null);
			}
			if (URLParser.parseURL(columnValue)) {
				return new ColumnType(DataType.link, null);
			}

			if (validLenght) {
				try {
					LocalDateTime dateTime = JavaDateTime.parse(columnValue);
					if (dateTime != null) {
						return new ColumnType(DataType.date, null);
					}
				} catch (ParseException e1) {
				}

			}
			if (validLenght && NumberParser.parseNumeric(columnValue)) {
				Unit unit = headerUnit;
				if (headerUnit == null) {
					unit = UnitParser.checkUnit(columnValue);
				}
				return new ColumnType(DataType.unit, unit);
			}
		}
		return new ColumnType(DataType.string, null);
	}

	@Override
	public ColumnType detectTypeForColumn(Object[] attributeValues, String attributeLabel) {

		HashMap<Object, Integer> typeCount = new HashMap<>();
		HashMap<Object, Integer> unitCount = new HashMap<>();
		Unit unit = UnitParser.parseUnitFromHeader(attributeLabel);
		// detect types and units per value
		int rowCounter = 0; // Skip first line --> header
		for (Object attribute : attributeValues) {
			if (rowCounter != 0) {
				String value = (String) attribute;
				ColumnType cdt = null;

				if (value != null) {
					cdt = guessTypeForValue(value, unit);
				}

				if (cdt != null) {
					MapUtils.increment(typeCount, cdt.getType());
					MapUtils.increment(unitCount, cdt.getUnit());
				}
			}
			rowCounter++;
		}

		// create default order to guarantee that results are reproducible in case multiple types have the same number of votes
		List<Pair<Object, Integer>> typeVotes = new ArrayList<>(typeCount.size());
		for(Object type : DataType.values()) {
			Integer count = typeCount.get(type);
			if(count!=null) {
				typeVotes.add(new Pair<>(type, count));
			}
		}
		
		// majority vote for type
//		Object type = MapUtils.max(typeCount);
		Pair<Object, Integer> maxPair = Q.max(typeVotes, (p) -> p.getSecond());
		Object type = null;
		if(maxPair!=null) {
			type = maxPair.getFirst();
		}
		if (type == null) {
			type = DataType.string;
		}

		// majority vote for Unit - if header unit empty
		if (unit == null) {
			unit = (Unit) MapUtils.max(unitCount);
		}

		ColumnType resColumnType = new ColumnType((DataType) type, unit);
		return resColumnType;
	}
}
