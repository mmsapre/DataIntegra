package com.integration.em.datatypes;
import java.util.regex.Pattern;

public class NumberParser {

    final static  String Digits = "(\\p{Digit}+)";
    final static  String HexDigits = "(\\p{XDigit}+)";

    final static  String Exp = "[eE][+-]?" + Digits;
    final static Pattern fpRegex = Pattern.compile(("[\\x00-\\x20]*" +
            "[+-]?(" +
            "NaN|" +
            "Infinity|" +

            "(((" + Digits + "(\\.)?(" + Digits + "?)(" + Exp + ")?%?)|" +

            "(\\.(" + Digits + ")(" + Exp + ")?)|" +

            "((" +
            "(0[xX]" + HexDigits + "(\\.)?)|" +


            "(0[xX]" + HexDigits + "?(\\.)" + HexDigits + ")" +

            ")[pP][+-]?" + Digits + "))" + "[fFdD]?))" + "[\\x00-\\x20]*"), Pattern.CASE_INSENSITIVE);


    public static boolean parseNumeric(String text) {
        if(canParseDouble(text)) {
            return true;
        } else {
            return parseByChar(text);
        }
    }


    private static boolean canParseDouble(String text) {
        return fpRegex.matcher(text).matches();
    }

    private static boolean parseByChar(String text) {

        int nmNumbers = 0;
        int nmChars = 0;

        for (char ch : text.toCharArray()) {
            if (Character.isDigit(ch))
                nmNumbers++;
            else if (!Character.isWhitespace(ch))
                nmChars++;
        }
        if (nmNumbers >= 1.5 * nmChars)
            return true;
        return false;
    }
}
