package com.integration.em.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.Collection;

@Slf4j
public class TblStringUtils {

    public static String join(Collection<?> values, String delimiter)
    {
        log.info("test ..........");
        StringBuilder sb = new StringBuilder();

        boolean first=true;
        for(Object value : values)
        {
            if(!first)
                sb.append(delimiter);

            if(value!=null) {
                sb.append(value.toString());
            } else {
                sb.append("null");
            }

            first = false;
        }

        return sb.toString();
    }

    public static String join(Object[] values, String delimiter)
    {
        StringBuilder sb = new StringBuilder();

        boolean first=true;
        for(Object value : values)
        {
            if(!first)
                sb.append(delimiter);

            if(value!=null) {
                sb.append(value.toString());
            } else {
                sb.append("null");
            }

            first = false;
        }

        return sb.toString();
    }

    public static boolean containsAny(String value, Collection<String> testValues)
    {
        if(value==null)
            return false;

        for (String s : testValues)
            if (value.contains(s))
                return true;
        return false;
    }
}
