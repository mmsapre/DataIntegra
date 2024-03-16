package com.integration.em.detect;

import java.util.*;

public class OthOps {

    public <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
            Map<K, V> map) {

        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(
                map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {

            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> entriesSortedByValues(
            Map<K, V> map) {
        SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<Map.Entry<K, V>>(
                new Comparator<Map.Entry<K, V>>() {
                    public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
                        int res = e1.getValue().compareTo(e2.getValue());
                        return res != 0 ? res : 1;
                    }
                });
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }

    public String getColumnContentWithoutSpaces(String[] column) {
        String content = "";
        for (String cell : column) {
            if (cell == null)
                continue;
            else {
                if ((!cell.trim().isEmpty()) && (!cell.trim().equals("-")
                        && !cell.trim().equals("--")
                        && !cell.trim().equals("---")
                        && !cell.trim().equals("n/a")
                        && !cell.trim().equals("N/A")
                        && !cell.trim().equals("(n/a)")
                        && !cell.trim().equals("Unknown")
                        && !cell.trim().equals("unknown")
                        && !cell.trim().equals("?") && !cell.trim().equals("??")
                        && !cell.trim().equals(".")
                        && !cell.trim().equals("null")
                        && !cell.trim().equals("NULL")
                        && !cell.trim().equals("Null"))) {
                    content += cell;
                } else {
                    continue;
                }
            }
        }
        content = content.replaceAll("\\s", "");
        return content;
    }
}
