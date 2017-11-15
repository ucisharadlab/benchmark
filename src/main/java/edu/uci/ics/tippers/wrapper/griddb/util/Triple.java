/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.wrapper.griddb.util;

import java.util.Objects;

//TODO: Remove and use apache commons lang3 instead
public class Triple<T1, T2, T3> {
    public T1 first;
    public T2 second;
    public T3 third;

    public Triple(T1 first, T2 second, T3 third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    @Override
    public String toString() {
        return first + "," + second + ", " + third;
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second, third);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Triple<?, ?, ?>)) {
            return false;
        }
        Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
        return Objects.equals(first, triple.first) && Objects.equals(second, triple.second)
                && Objects.equals(third, triple.third);
    }

}
