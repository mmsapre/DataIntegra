package com.integration.em.tables;


import com.integration.em.utils.Function;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
public class Pair<T,U> implements Serializable {

    private T first;
    private U second;

    public Pair() {

    }

    public Pair(T first, U second) {
        this.first = first;
        this.second = second;
    }

    public T getFirst() {
        return first;
    }

    public U getSecond() {
        return second;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((first == null) ? 0 : first.hashCode());
        result = prime * result + ((second == null) ? 0 : second.hashCode());
        return result;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Pair))
            return false;
        Pair other = (Pair) obj;
        if (first == null) {
            if (other.first != null)
                return false;
        } else if (!first.equals(other.first))
            return false;
        if (second == null) {
            if (other.second != null)
                return false;
        } else if (!second.equals(other.second))
            return false;
        return true;
    }

    public static <T,U> Map<T, U> toMap(Collection<Pair<T, U>> pairs) {
        Map<T, U> result = new HashMap<>();

        for(Pair<T, U> p : pairs) {
            result.put(p.getFirst(), p.getSecond());
        }

        return result;
    }

    public static <T,U> Map<T, U> mergeToMap(Collection<Pair<T, U>> pairs, Function<U,Pair<U,U>> merge) {
        Map<T, U> result = new HashMap<>();

        for(Pair<T, U> p : pairs) {
            if(result.containsKey(p.getFirst())) {
                result.put(p.getFirst(), merge.execute(new Pair<>(result.get(p.getFirst()), p.getSecond())));
            } else {
                result.put(p.getFirst(), p.getSecond());
            }
        }

        return result;
    }

    public static <T,U> Collection<Pair<T,U>> fromMap(Map<T,U> map) {
        Collection<Pair<T,U>> result = new ArrayList<>();

        for(T key : map.keySet()) {
            result.add(new Pair<T, U>(key, map.get(key)));
        }

        return result;
    }

    @Override
    public String toString() {
        return String.format("(%s,%s)", first, second);
    }
}
