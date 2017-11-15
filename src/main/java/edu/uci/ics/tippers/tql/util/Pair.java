package edu.uci.ics.tippers.tql.util;

import java.io.Serializable;
import java.util.Objects;

//TODO: Remove and use apache commons lang3 instead
public class Pair<T1, T2> implements Serializable {

    private static final long serialVersionUID = 1L;
    public T1 first;
    public T2 second;

    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return first + "," + second;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair)) {
            return false;
        }
        Pair<?, ?> p = (Pair<?, ?>) obj;
        return Objects.equals(first, p.first) && Objects.equals(second, p.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}