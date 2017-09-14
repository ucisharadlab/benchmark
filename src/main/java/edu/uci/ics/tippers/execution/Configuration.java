package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.util.*;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

public class Configuration {

    private List<Database> databases = new ArrayList<>();
    private Map<Database, List<Integer>> mappings = new HashMap<>();
    private Preferences preferences;

    public Configuration(Preferences preferences) throws BenchmarkException {
        this.preferences = preferences;
        readPreferences();
    }

    public void readPreferences() throws BenchmarkException {

        // Reading Database List
        String nodeValue = preferences.node("benchmark").get("databases", null);
        if (nodeValue == null) {
            throw new BenchmarkException("Database list not provided in configuration file");
        }

        databases = Arrays.stream(nodeValue.split(",")).map(e->Database.get(e)).collect(Collectors.toList());

        // Reading Mapping List
        for(Database db: databases) {
            nodeValue = preferences.node(db.getName()).get("mapping", null);
            if (nodeValue == null) {
                throw new BenchmarkException("Mapping list not provided in configuration file");
            }
            mappings.put(db, Arrays.stream(nodeValue.split(","))
                    .map(e->Integer.parseInt(e)).collect(Collectors.toList()));
        }
    }

    public List<Database> getDatabases() {
        return databases;
    }

    public void setDatabases(List<Database> databases) {
        this.databases = databases;
    }

    public Map<Database, List<Integer>> getMappings() {
        return mappings;
    }

    public void setMappings(Map<Database, List<Integer>> mappings) {
        this.mappings = mappings;
    }
}
