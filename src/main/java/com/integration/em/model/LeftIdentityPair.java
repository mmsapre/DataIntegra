package com.integration.em.model;

import com.integration.em.tables.Pair;

public class LeftIdentityPair<T, U> extends Pair<T, U> {

    public LeftIdentityPair() {
        super();
    }

    public LeftIdentityPair(T first, U second) {
        super(first, second);
    }

    @Override
    public int hashCode() {
        return getFirst().hashCode();
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
        if (getFirst() == null) {
            if (other.getFirst() != null)
                return false;
        } else if (!getFirst().equals(other.getFirst()))
            return false;
        return true;
    }
}
