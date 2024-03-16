package com.integration.em.utils;

import java.util.Comparator;
import java.util.*;
import java.util.function.Function;

public class MapUtils {

    public static <T> Integer increment(Map<T, Integer> map, T keyValue) {
        Integer cnt = map.get(keyValue);

        if(cnt==null) {
            cnt = 0;
        }

        map.put(keyValue, cnt+1);

        return cnt+1;
    }

    public static <T> void add(Map<T, Integer> map, T keyValue, int value) {
        Integer cnt = map.get(keyValue);

        if(cnt==null) {
            cnt = 0;
        }

        map.put(keyValue, cnt+value);
    }

    public static <T> void add(Map<T, Double> map, T keyValue, double value) {
        Double sum = map.get(keyValue);

        if(sum==null) {
            sum = 0.0;
        }

        map.put(keyValue, sum+value);
    }

    public static <T, U extends Comparable<U>> T max(Map<T, U> map) {
        U iMax = null;
        T tMax = null;

        for(T t : map.keySet()) {
            U i = map.get(t);

//			if(iMax==null || i>iMax) {
            if(iMax==null || i.compareTo(iMax)>0) {
                iMax = i;
                tMax = t;
            }
        }

        return tMax;
    }


    public static <T, U> U get(Map<T, U> map, T keyValue, U defaultValue) {
        U val = map.get(keyValue);

        if(val==null) {
            map.put(keyValue, defaultValue);
            return defaultValue;
        } else {
            return val;
        }
    }


    public static <T, U> U getFast(Map<T, U> map, T keyValue, Function<T, U> createDefaultValue) {
        U val = map.get(keyValue);

        if(val==null) {
            val = createDefaultValue.apply(keyValue);
            map.put(keyValue, val);
        }

        return val;
    }

    public static <K, V> List<Map.Entry<K, V>> sort(Map<K, V> map, Comparator<Map.Entry<K, V>> comparator) {
        ArrayList<Map.Entry<K, V>> sorted = new ArrayList<>(map.size());
        for(Map.Entry<K, V> entry : map.entrySet()) {
            sorted.add(entry);
        }
        Collections.sort(sorted, comparator);
        return sorted;
    }


    public static <K, V> Map<V, K> invert(Map<K, V> map) {
        HashMap<V, K> inverted = new HashMap<>();

        for(K key : map.keySet()) {
            inverted.put(map.get(key), key);
        }

        return inverted;
    }
}
