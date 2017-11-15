/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.util;

public class Counter {
    private int counter = 0;

    public Counter() {
    }

    public Counter(int initial) {
        counter = initial;
    }

    public int get() {
        return counter;
    }

    public void inc() {
        ++counter;
    }

    public void set(int newStart) {
        counter = newStart;
    }
}
