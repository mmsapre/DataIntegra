package com.integration.em.datatypes;
import com.integration.em.tables.Pair;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Locale;
import java.util.regex.Pattern;
public class JavaDateTime {

    private static final Collection<Pair<Pattern, String>> DATE_FORMAT_REGEXPS = new LinkedList<>();

    static {

        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}-##-##$", Pattern.CASE_INSENSITIVE), "yyyy"));


        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}-\\d{2}-##$", Pattern.CASE_INSENSITIVE), "yyyy-MM"));

        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}$", Pattern.CASE_INSENSITIVE), "yyyy"));

        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{8}$", Pattern.CASE_INSENSITIVE), "yyyyMMdd"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\.\\d{1,2}\\.\\d{4}$", Pattern.CASE_INSENSITIVE), "dd.MM.yyyy"));
//		DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-(\\d{1,2}|[a-z]+)-\\d{4}$", Pattern.CASE_INSENSITIVE), "dd-MM-yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-\\d{1,2}-\\d{4}$", Pattern.CASE_INSENSITIVE), "dd-MM-yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}/\\d{1,2}/\\d{4}$", Pattern.CASE_INSENSITIVE), "dd/MM/yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\.\\d{1,2}\\.\\d{2}$", Pattern.CASE_INSENSITIVE), "dd.MM.yy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-\\d{1,2}-\\d{2}$", Pattern.CASE_INSENSITIVE), "dd-MM-yy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}/\\d{1,2}/\\d{2}$", Pattern.CASE_INSENSITIVE), "dd/MM/yy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\.\\d{4}$", Pattern.CASE_INSENSITIVE), "MM.yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-\\d{4}$", Pattern.CASE_INSENSITIVE), "MM-yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}/\\d{4}$", Pattern.CASE_INSENSITIVE), "MM/yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\.\\d{2}$", Pattern.CASE_INSENSITIVE), "MM.yy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-\\d{2}$", Pattern.CASE_INSENSITIVE), "MM-yy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}/\\d{2}$", Pattern.CASE_INSENSITIVE), "MM/yy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$", Pattern.CASE_INSENSITIVE), "yyyy-MM-dd"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}-\\d{1}-\\d{1}$", Pattern.CASE_INSENSITIVE), "yyyy-M-d"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}/\\d{1,2}/\\d{4}$", Pattern.CASE_INSENSITIVE), "MM/dd/yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}/\\d{1,2}/\\d{1,2}$", Pattern.CASE_INSENSITIVE), "yyyy/MM/dd"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", Pattern.CASE_INSENSITIVE), "dd MMM yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", Pattern.CASE_INSENSITIVE), "dd MMMM yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^[a-z]{4,}\\s\\d{1,2}\\s\\d{4}$", Pattern.CASE_INSENSITIVE), "MMMM dd yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-[a-z]{4,}-\\d{4}$", Pattern.CASE_INSENSITIVE), "dd-MMMM-yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\.[a-z]{4,}\\.\\d{4}$", Pattern.CASE_INSENSITIVE), "dd.MMMM.yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{4,}$", Pattern.CASE_INSENSITIVE), "dd MMMM"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^[a-z]{4,}\\s\\d{1,2}$", Pattern.CASE_INSENSITIVE), "MMMM dd"));

        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{2,}$", Pattern.CASE_INSENSITIVE), "dd MMMM"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-[a-z]{2,}$", Pattern.CASE_INSENSITIVE), "dd-MMMM"));

        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{2,}\\s\\d{4}$", Pattern.CASE_INSENSITIVE), "dd MMMM yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}/[a-z]{2,}/\\d{4}$", Pattern.CASE_INSENSITIVE), "dd/MMMM/yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1}-[a-z]{3}-\\d{4}$", Pattern.CASE_INSENSITIVE), "d-MMM-yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-[a-z]{3}-\\d{4}$", Pattern.CASE_INSENSITIVE), "dd-MMM-yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-[a-z]{3,}-\\d{4}$", Pattern.CASE_INSENSITIVE), "dd-MMMM-yyyy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\.[a-z]{2,}\\.\\d{4}$", Pattern.CASE_INSENSITIVE), "dd.MMMM.yyyy"));

        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{2,}\\s\\d{2}$", Pattern.CASE_INSENSITIVE), "dd MMMM yy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}/[a-z]{2,}/\\d{2}$", Pattern.CASE_INSENSITIVE), "dd/MMMM/yy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-[a-z]{2,}-\\d{2}$", Pattern.CASE_INSENSITIVE), "dd-MMMM-yy"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\.[a-z]{2,}\\.\\d{2}$", Pattern.CASE_INSENSITIVE), "dd.MMMM.yy"));

        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{12}$", Pattern.CASE_INSENSITIVE), "yyyyMMddHHmm"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{8}\\s\\d{4}$", Pattern.CASE_INSENSITIVE), "yyyyMMdd HHmm"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "dd-MM-yyyy HH:mm"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "yyyy-MM-dd HH:mm"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "MM/dd/yyyy HH:mm"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "yyyy/MM/dd HH:mm"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "dd MMM yyyy HH:mm"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "dd MMMM yyyy HH:mm"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{14}$", Pattern.CASE_INSENSITIVE), "yyyyMMddHHmmss"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{8}\\s\\d{6}$", Pattern.CASE_INSENSITIVE), "yyyyMMdd HHmmss"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "dd-MM-yyyy HH:mm:ss"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "yyyy-MM-dd HH:mm:ss"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "MM/dd/yyyy HH:mm:ss"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "yyyy/MM/dd HH:mm:ss"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "dd MMM yyyy HH:mm:ss"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", Pattern.CASE_INSENSITIVE),
                "dd MMMM yyyy HH:mm:ss"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{6}$",
                Pattern.CASE_INSENSITIVE), "dd MMMM yyyy HH:mm:ss.SSSSSS"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s\\d{2}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{6}$",
                Pattern.CASE_INSENSITIVE), "dd MM yyyy HH:mm:ss.SSSSSS"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}\\s\\d{2}\\s\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{6}$",
                Pattern.CASE_INSENSITIVE), "yyyy MM dd HH:mm:ss.SSSSSS"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}-\\d{2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{6}$", Pattern.CASE_INSENSITIVE),
                "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{2}$",
                Pattern.CASE_INSENSITIVE), "dd MMMM yyyy HH:mm:ss.SS"));

        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}-\\d{2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2}$",
                Pattern.CASE_INSENSITIVE), "yyyy-MM-dd'T'HH:mm:ssXXX"));
        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\d{4}-\\d{2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}Z$", Pattern.CASE_INSENSITIVE),
                "yyyy-MM-dd'T'HH:mm:ssXXX"));

        DATE_FORMAT_REGEXPS.add(new Pair<>(Pattern.compile("^\\+\\d{4}-\\d{2}-\\d{1,2}T\\d{1,2}:\\d{2}:\\d{2}Z$", Pattern.CASE_INSENSITIVE),
                "'+'yyyy-MM-dd'T'HH:mm:ssXXX"));

    }
    public static LocalDateTime parse(String dateString, String dateFormat) throws ParseException {
        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendPattern(dateFormat)
                .parseDefaulting(ChronoField.YEAR_OF_ERA, 1).parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                .parseDefaulting(ChronoField.DAY_OF_MONTH, 1).parseDefaulting(ChronoField.CLOCK_HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0).parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter(Locale.ENGLISH);

        return LocalDateTime.parse(dateString, formatter);
    }

    public static String determineDateFormat(String dateString) {
        for (Pair<Pattern,String> regexp : DATE_FORMAT_REGEXPS) {
            if (regexp.getFirst().matcher(dateString).matches()) {
                return regexp.getSecond();
            }
        }
        return null; // Unknown format.
    }
    public static LocalDateTime parse(String dateString) throws ParseException {
        if (dateString == null) {
            return null;
        }
        try {
            double possibleHeight = Double.parseDouble(dateString);
            if (possibleHeight > 1.5 && possibleHeight < 2.5) {
                return null;
            }
        } catch (Exception e) {
        }
        try {
            return LocalDateTime.parse(dateString);

        } catch (DateTimeParseException e) {

            String dateFormat = determineDateFormat(dateString);
            if (dateFormat == null) {
                throw new ParseException("Unknown date format.", 0);
            }
            if (dateString.contains("-##")) {
                dateString = dateString.replace("-##", "");
            }
            LocalDateTime d = null;
            if (dateFormat.contains("MM") && dateFormat.contains("dd")) {
                try {
                    d = parse(dateString, dateFormat);
                } catch (Exception e1) {
                    String util = dateFormat.replace("MM", "XX");
                    util = util.replace("dd", "MM");
                    util = util.replace("XX", "dd");
                    try {
                        d = parse(dateString, util);
                    } catch (Exception e2) {
                    }
                }
                return d;
            }
            try {
                d = parse(dateString, dateFormat);
            } catch (Exception e3) {
            }

            if (d != null && (d.getYear() < 0 || d.getYear() > 2100)) {
                return null;
            }
            return d;
        }
    }
}
