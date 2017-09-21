package edu.uci.ics.tippers.common;

import java.util.HashMap;
import java.util.Map;

public enum ReportFormat {

    TEXT("text"),
    HTML("html"),
    PDF("pdf");

    private final String name;

    private static final Map<String, ReportFormat> lookup = new HashMap<>();

    static {
        for (ReportFormat d : ReportFormat.values()) {
            lookup.put(d.getName(), d);
        }
    }

    private ReportFormat(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static ReportFormat get(String name) {
        return lookup.get(name.toLowerCase());
    }

}
