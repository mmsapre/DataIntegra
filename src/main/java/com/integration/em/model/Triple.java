package com.integration.em.model;

public class Triple<TFirst,TSecond,TThird> {

    private TFirst first;
    private TSecond second;
    private TThird third;

    public Triple(TFirst first, TSecond second, TThird third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public TFirst getFirst() {
        return first;
    }

    public void setFirst(TFirst first) {
        this.first = first;
    }

    public TSecond getSecond() {
        return second;
    }

    public void setSecond(TSecond second) {
        this.second = second;
    }

    public TThird getThird() {
        return third;
    }

    public void setThird(TThird third) {
        this.third = third;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((first == null) ? 0 : first.hashCode());
        result = prime * result + ((second == null) ? 0 : second.hashCode());
        result = prime * result + ((third == null) ? 0 : third.hashCode());
        return result;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Triple other = (Triple) obj;
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
        if (third == null) {
            if (other.third != null)
                return false;
        } else if (!third.equals(other.third))
            return false;
        return true;
    }
}
