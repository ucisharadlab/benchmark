package edu.uci.ics.tippers.common;

import java.util.HashMap;
import java.util.Map;

public enum Database {

    MONGODB ("mongodb"),
    GRIDDB ("griddb") ,
    ASTERIXDB ("asterixdb"),
    CRATEDB ("cratedb"),
    CASSANDRA ("cassandra"),
    POSTGRESQL ("postgresql");

    private final String name;

    private static final Map<String, Database> lookup = new HashMap<>();

    static {
        for (Database d : Database.values()) {
            lookup.put(d.getName(), d);
        }
    }

    private Database(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static Database get(String name) {
        return lookup.get(name.toLowerCase());
    }

}
