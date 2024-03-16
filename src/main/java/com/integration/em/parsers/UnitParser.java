package com.integration.em.parsers;

import com.integration.em.datatypes.Unit;

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

public class UnitParser {

    public static List<Unit> units = new ArrayList<>();


    public static List<Unit> getUnits() {
        return units;
    }


    public static Unit getUnit(String name) {
        for(Unit unit: units){
            if(unit.getName().equals(name)){
                return unit;
            }
        }
        return null;
    }

    public static Double parseUnit(String value, String unitInformation) {
        for (Unit unit : units) {
            if (!unitInformation.isEmpty()) {
                if (unitInformation.toLowerCase().equals(unit.getName())
                        || unit.getAbbreviations().contains(unitInformation.toLowerCase())) {
                    value = value.replaceAll("[^0-9\\,\\.\\-Ee\\+]", "");
                    Double valueBeforeTransformation = Double.parseDouble(value);
                    return valueBeforeTransformation * unit.getFactor();
                }
            } else {
                String nonNumberPart = value.replaceAll("[0-9\\,\\.\\-Ee\\+]", "");
                if (nonNumberPart.toLowerCase().equals(unit.getName())
                        || unit.getAbbreviations().contains(nonNumberPart.toLowerCase())) {
                    value = value.replaceAll("[^0-9\\,\\.\\-Ee\\+]", "");
                    Double valueBeforeTransformation = Double.parseDouble(value);
                    return valueBeforeTransformation * unit.getFactor();
                } else {
                    value = value.replaceAll("[^0-9\\,\\.\\-Ee\\+]", "");
                    Double valueBeforeTransformation = Double.parseDouble(value);
                    return valueBeforeTransformation;
                }
            }
        }
        return null;
    }

    public static Double transformUnit(String value, Unit unit) throws ParseException {
        value = value.replaceAll("[^0-9\\,\\.\\-Ee\\+]", "");
        NumberFormat format = NumberFormat.getInstance(Locale.US);
        Number number = format.parse(value);
        Double valueBeforeTransformation = number.doubleValue();
        return valueBeforeTransformation * unit.getFactor();
    }

    public static Unit checkUnit(String value) {
        for (Unit unit : units) {

            String nonNumberPart = value.replaceAll("[0-9\\,\\.\\-Ee\\+]", "");
            nonNumberPart = nonNumberPart.trim();
            if (nonNumberPart.toLowerCase().equals(unit.getName())
                    || unit.getAbbreviations().contains(nonNumberPart.toLowerCase())) {
                return unit;
            }
        }
        return null;
    }

    private static Pattern unitInHeaderPattern = Pattern.compile(".*\\((.*)\\).*");
    private static Pattern dotPattern = Pattern.compile("\\.");

    public static Unit parseUnitFromHeader(String header) {
        String unitName = extractUnitAbbrFromHeader(header).toLowerCase();

        for (Unit unit : units) {
            if (!header.isEmpty()) {
                if (header.toLowerCase().equals(unit.getName())
                        || unit.getAbbreviations().contains(header.toLowerCase())
                        || unitName.equals(unit.getName())
                        || unit.getAbbreviations().contains(unitName)) {
                    return unit;
                }
            }
        }

        return null;
    }

    private static String extractUnitAbbrFromHeader(String header) {
        try {
            Matcher m = unitInHeaderPattern.matcher(header);
            if (m.matches()) {
                String unit = m.group(1);

                return dotPattern.matcher(unit).replaceAll("");
            }
        } catch (Exception e) {
        }

        return header;
    }

    static {
        initialiseUnits();
    }

    private static void initialiseUnits() {
        synchronized (units) {
            if (units.isEmpty()) {
                try {

                    URI uri = UnitParser.class.getResource("Units/Convertible").toURI();
                    Path myPath;
                    if (uri.getScheme().equals("jar")) {
                        FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.<String, Object>emptyMap());
                        myPath = fileSystem.getPath("/resources");
                    } else {
                        myPath = Paths.get(uri);
                    }

                    Files.walkFileTree(myPath, new HashSet<FileVisitOption>(), 1, new FileVisitor<Path>() {

                        @Override
                        public FileVisitResult preVisitDirectory(Path dir,
                                                                 BasicFileAttributes attrs) throws IOException {
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(Path file,
                                                         BasicFileAttributes attrs) throws IOException {
                            //System.out.println(file.toFile().getName());
                            units.addAll(readConvertibleUnit(file.toFile()));
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(Path file, IOException exc)
                                throws IOException {
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                                throws IOException {
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                }
                catch (URISyntaxException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static Set<Unit> readConvertibleUnit(File unitPath) {
        Set<Unit> unitsOfFile = new HashSet<>();
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    new FileInputStream(unitPath), "UTF8"));
            String fileLine = in.readLine();
            while (fileLine != null) {
                Unit currentUnit = new Unit();
                String[] parts = fileLine.split("\\|");
                currentUnit.setName(parts[0].replace("\"", ""));
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
                fileLine = in.readLine();
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return unitsOfFile;
    }
}
