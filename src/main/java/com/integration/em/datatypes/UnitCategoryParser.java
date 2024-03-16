package com.integration.em.datatypes;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class UnitCategoryParser {

	public static List<UnitCategory> categories = new ArrayList<>();
	public static List<Quantity> quantities = new ArrayList<>();

	public static List<UnitCategory> getUnitCategories() {
		return categories;
	}


	public static Quantity getQuantity(String name) {
		for (Quantity quantity : quantities) {
			if (quantity.getName().equals(name)) {
				return quantity;
			}
		}
		return null;
	}


	public static UnitCategory getUnitCategory(String name) {
		for (UnitCategory unitCategory : categories) {
			if (unitCategory.getName().equals(name)) {
				return unitCategory;
			}
		}
		return null;
	}

	public static Double transform(String value, Unit unit, Quantity quantity) throws ParseException {

		String reducedValue = value;
		// Remove quantity description
		if (quantity != null) {
			reducedValue = value.replace(quantity.getName(), "");
			for (String abbr : quantity.getAbbreviations()) {
				reducedValue = reducedValue.replace(abbr, "");
			}
		}

		// Remove unit description
		if (unit != null) {
			reducedValue = value.replace(unit.getName(), "");
			for (String abbr : unit.getAbbreviations()) {
				reducedValue = reducedValue.replace(abbr, "");
			}
		}

		Double normalizedValue = normalizeNumeric(reducedValue);

		if (quantity != null) {
			normalizedValue *= quantity.getFactor();
		}

		if (unit != null) {
			normalizedValue *= unit.getFactor();
		}

		return normalizedValue;
	}

	public static Unit checkUnit(String value, UnitCategory unitCategory) {
		if (unitCategory != null) {
			Quantity quantity = checkQuantity(value);

			if (quantity != null) {
				try {
					value = transformQuantity(value, quantity);
				} catch (ParseException e) {
					log.trace("Could not transform " + value + "!");
					return null;
				}
			}

			for (Unit unit : unitCategory.getUnits()) {
				String nonNumberPart = value.replaceAll("^[0-9\\,\\.\\-Ee\\+]*", "");
				nonNumberPart = nonNumberPart.trim();
				if (nonNumberPart.toLowerCase().equals(unit.getName())
						|| unit.getAbbreviations().contains(nonNumberPart.toLowerCase())) {
					return unit;
				}
			}
		}
		return null;
	}

	public static String transformQuantity(String value, Quantity quantity) throws ParseException {
		String reducedValue = value.replaceAll("[^0-9\\,\\.\\-Ee\\+]", "");

		Double valueBeforeTransformation = normalizeNumeric(reducedValue);
		Double transformedValue = valueBeforeTransformation * quantity.getFactor();

		value = value.replaceAll(quantity.getName(), "");
		value = value.replaceAll(reducedValue, "");

		value = transformedValue + value;

		return value;
	}

	public static Quantity checkQuantity(String value) {
		for (Quantity quantity : quantities) {

			String nonNumberPart = value.replaceAll("[0-9\\,\\.\\-Ee\\+]", "");
			nonNumberPart = nonNumberPart.trim();
			if (nonNumberPart.toLowerCase().contains(quantity.getName())
					|| quantity.getAbbreviations().contains(nonNumberPart.toLowerCase())) {
				return quantity;
			}
		}
		return null;
	}

	public static UnitCategory checkUnitCategory(String value) {

		for (UnitCategory category : categories) {
			Unit unit = checkUnit(value, category);
			if (unit != null) {
				return unit.getUnitCategory();
			}
		}

		return null;

	}

	public static Double normalizeNumeric(String value) throws ParseException {
		String reducedValue = value.replaceAll("[^0-9\\,\\.\\-Ee\\+]", "");

		NumberFormat format = NumberFormat.getInstance(Locale.US);
		Number number = format.parse(reducedValue);
		return number.doubleValue();
	}

	private static Pattern unitInHeaderPattern = Pattern.compile(".*\\((.*)\\).*");
	private static Pattern dotPattern = Pattern.compile("\\.");

	public static Unit parseUnitFromHeader(String header) {
		String unitName = extractUnitAbbrFromHeader(header).toLowerCase();

		for (UnitCategory category : categories) {
			for (Unit unit : category.getUnits()) {
				if (!header.isEmpty()) {
					if (header.toLowerCase().equals(unit.getName())
							|| unit.getAbbreviations().contains(header.toLowerCase()) || unitName.equals(unit.getName())
							|| unit.getAbbreviations().contains(unitName)) {
						return unit;
					}
				}
			}
		}
		return null;
	}

	private static String extractUnitAbbrFromHeader(String header) {
		try {
			// if (header.matches(".*\\(.*\\).*")) {
			Matcher m = unitInHeaderPattern.matcher(header);
			if (m.matches()) {
				String unit = m.group(1);

				return dotPattern.matcher(unit).replaceAll("");
				// return header.substring(header.indexOf("(") + 1,
				// header.indexOf(")")).replaceAll("\\.", "");
			}
		} catch (Exception e) {
		}

		return header;
	}

	static {
		initialiseUnitCategories();
	}

	private static void initialiseUnitCategories() {
		synchronized (categories) {
			if (categories.isEmpty()) {
				try {

					UnitCategory all = new UnitCategory("All");
					categories.add(all);

					URI uri = UnitCategoryParser.class.getResource("Units/Convertible").toURI();
					Path myPath;
					if (uri.getScheme().equals("jar")) {
						FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.<String, Object>emptyMap());
						myPath = fileSystem.getPath("/resources");
					} else {
						myPath = Paths.get(uri);
					}

					Files.walkFileTree(myPath, new HashSet<FileVisitOption>(), 1, new FileVisitor<Path>() {

						@Override
						public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
								throws IOException {
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
							// System.out.println(file.toFile().getName());

							UnitCategory unitCategory = initialiseUnitCategory(file);
							if (unitCategory.getName().equals("Quantity")) {
								quantities.addAll(readQuantity(file.toFile()));
							} else {
								categories.add(unitCategory);
								readConvertibleUnit(file.toFile(), unitCategory);
							}
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
							return FileVisitResult.CONTINUE;
						}
					});
				} catch (IOException e) {
					e.printStackTrace();
				} catch (URISyntaxException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static Set<Unit> readConvertibleUnit(File unitPath, UnitCategory unitCategory) {
		Set<Unit> unitsOfFile = new HashSet<>();
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(unitPath), "UTF8"));
			String fileLine = in.readLine();

			UnitCategory all = categories.get(0);

			while (fileLine != null) {
				Unit currentUnit = new Unit();
				String[] parts = fileLine.split("\\|");
				currentUnit.setName(parts[0].replace("\"", ""));
				currentUnit.setUnitCategory(unitCategory);

				// List<String> abbs = new ArrayList();
				HashSet<String> abbs = new HashSet<>();
				String[] subUnitsStrs = parts[1].split(",");
				for (String s : subUnitsStrs) {
					abbs.add(s.replace("\"", ""));
				}
				currentUnit.setAbbreviations(abbs);
				if (parts.length < 3) {
					currentUnit.setFactor(1.0);
				} else {
					currentUnit.setFactor(Double.parseDouble(parts[2]));
				}
				unitsOfFile.add(currentUnit);
				unitCategory.addUnit(currentUnit);
				all.addUnit(currentUnit);
				fileLine = in.readLine();
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return unitsOfFile;
	}

	private static Set<Quantity> readQuantity(File unitPath) {
		Set<Quantity> quantitysOfFile = new HashSet<>();
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(unitPath), "UTF8"));
			String fileLine = in.readLine();
			while (fileLine != null) {
				Quantity currentQuantity = new Quantity();
				String[] parts = fileLine.split("\\|");
				currentQuantity.setName(parts[0].replace("\"", ""));

				// List<String> abbs = new ArrayList();
				HashSet<String> abbs = new HashSet<>();
				String[] subUnitsStrs = parts[1].split(",");
				for (String s : subUnitsStrs) {
					abbs.add(s.replace("\"", ""));
				}
				currentQuantity.setAbbreviations(abbs);
				if (parts.length < 3) {
					currentQuantity.setFactor(1.0);
				} else {
					currentQuantity.setFactor(Double.parseDouble(parts[2]));
				}
				quantitysOfFile.add(currentQuantity);
				fileLine = in.readLine();
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return quantitysOfFile;
	}

	private static UnitCategory initialiseUnitCategory(Path filePath) {

		String simpleFileName = filePath.getFileName().toString();
		simpleFileName = simpleFileName.replace(".txt", "");

		UnitCategory unitCategory = new UnitCategory(simpleFileName);

		return unitCategory;
	}

}
