package com.integration.em.datatypes;
import com.integration.em.parsers.UnitParser;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
public class TypeConverter {

    private boolean verbose = false;
    public boolean isVerbose() {
        return verbose;
    }
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public Object typeValue(String value, DataType type, Unit unit) {
        Object typedValue = null;

        if(value!=null) {
            try {
                switch (type) {
                    case string:
                        typedValue = value;
                        break;
                    case date:
                        typedValue = JavaDateTime.parse(value);
                        break;
                    case number:
                        if (unit != null) {
                            typedValue = UnitParser.transformUnit(value, unit);

                        } else {
                            value = normaliseNumeric(value);
                            NumberFormat format = NumberFormat.getInstance(Locale.US);
                            Number number = format.parse(value);
                            typedValue = number.doubleValue();
                        }
                        break;
                    case bool:
                        typedValue = Boolean.parseBoolean(value);
                        break;
                    case align:
                        typedValue = value;
                        break;
                    case link:
                        typedValue = value;
                    default:
                        break;
                }
            } catch(ParseException e) {
                if(isVerbose()) {
                    e.printStackTrace();
                }
            }
        }

        return typedValue;
    }

    public static String normaliseNumeric(String value) {
        return value.replaceAll("[^0-9\\,\\.\\-Ee\\+]", "");
    }
}
