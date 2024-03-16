package com.integration.em.datatypes;

import java.util.regex.Pattern;

public class BooleanParser {

    public static final Pattern booleanRegex = Pattern.compile("(yes|true|1|no|false|0)", Pattern.CASE_INSENSITIVE);

    public static boolean parseBoolean(String text) {

        return booleanRegex.matcher(text).matches();
    }
}
