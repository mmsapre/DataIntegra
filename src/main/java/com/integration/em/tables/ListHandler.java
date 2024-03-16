package com.integration.em.tables;

import java.util.List;
import java.util.regex.Pattern;
public class ListHandler {

    private static final Pattern listPattern = Pattern.compile("^\\{.+\\}$");

    public static boolean checkIfList(String columnValue) {
        return columnValue != null && listPattern.matcher(columnValue).matches();
    }

    public static String[] splitList(String columnValue) {
        String data = columnValue.substring(1, columnValue.length() - 1);
        return data.split("\\|");
    }

    public static String formatList(List<String> values) {
        return formatList(values, "|");
    }

    public static String formatList(List<String> values, String separator) {
        StringBuilder sb = new StringBuilder();

        sb.append("{");

        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) != null) {
                if (i != 0) {
                    sb.append(separator);
                }

                sb.append(values.get(i).replace(separator, ""));
            }
        }

        sb.append("}");

        return sb.toString();
    }

    public static boolean isArray(final Object obj) {
        if (obj != null)
            return obj.getClass().isArray();
        return false;
    }
}
