package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;
import javafx.util.Pair;
import org.ini4j.Ini;
import org.ini4j.IniPreferences;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.prefs.Preferences;

/* Main class to be run to start the benchmark
 */
public class Benchmark {

    private static String CONFIG = "benchmark.ini";
    private static Map<Pair<Database, Integer>, Map<Integer, Long>> runTimes = new HashMap<>();
    private static Configuration configuration;

    private Configuration readConfiguration() throws BenchmarkException {
        try {
            Ini ini = new Ini(new File(getClass().getClassLoader().getResource(CONFIG).getFile()));
            Preferences prefs = new IniPreferences(ini);
            configuration = new Configuration(prefs);

        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error reading Configuration File");
        }
        return null;
    }


    public static void main(String args[]) {
        Benchmark benchmark = new Benchmark();

        try {
            benchmark.readConfiguration();
            for (Database database: configuration.getDatabases()) {
                switch (database) {
                    case GRIDDB:
                        break;
                    case CRATEDB:
                        break;
                    case MONGODB:
                        break;
                    case ASTERIXDB:
                        break;
                    case CASSANDRA:
                        break;
                    case POSTGRESQL:
                        break;
                    default:
                        throw new BenchmarkException("Database Not Supported");
                }
            }
        } catch (BenchmarkException e) {
            e.printStackTrace();
        }
    }

}
