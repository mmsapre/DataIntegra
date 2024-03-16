package com.integration.em.parsers;

import java.util.List;

public interface StringNormalizer {

    public static final String nullValue = "NULL";

    String normaliseHeader(String columnName);

    String normaliseValue(String value, boolean removeContentInBrackets);

    String normalise(String s, boolean useStemmer);

    List<String> tokenise(String s, boolean useStemmer);
}
