package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.data.asterixdb.AsterixDBDataUploader;
import edu.uci.ics.tippers.data.cassandra.CassandraDataUploader;
import edu.uci.ics.tippers.data.cratedb.CrateDBDataUploader;
import edu.uci.ics.tippers.data.griddb.GridDBDataUploader;
import edu.uci.ics.tippers.data.mongodb.MongoDBDataUploader;
import edu.uci.ics.tippers.data.postgresql.PgSQLDataUploader;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.query.asterixdb.AsterixDBQueryManager;
import edu.uci.ics.tippers.query.cassandra.CassandraQueryManager;
import edu.uci.ics.tippers.query.cratedb.CrateDBQueryManager;
import edu.uci.ics.tippers.query.griddb.GridDBQueryManager;
import edu.uci.ics.tippers.query.mongodb.MongoDBQueryManager;
import edu.uci.ics.tippers.query.postgresql.PgSQLQueryManager;
import edu.uci.ics.tippers.scaler.Scale;
import edu.uci.ics.tippers.schema.BaseSchema;
import edu.uci.ics.tippers.schema.asterixdb.AsterixDBSchema;
import edu.uci.ics.tippers.schema.cassandra.CassandraSchema;
import edu.uci.ics.tippers.schema.cratedb.CrateDBSchema;
import edu.uci.ics.tippers.schema.griddb.GridDBSchema;
import edu.uci.ics.tippers.schema.mongodb.MongoDBSchema;
import edu.uci.ics.tippers.schema.postgresql.PgSQLSchema;
import javafx.util.Pair;
import org.ini4j.Ini;
import org.ini4j.IniPreferences;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.prefs.Preferences;

/* Main class to be run to start the benchmark
 */
public class Benchmark {

    private static final String OBJECTS = "objects/";
    private static final String ROWS = "rows/";
    private static final String MIX = "mix/";

    private static Map<Pair<Database, Integer>, Map<Integer, Duration>> runTimes = new HashMap<>();
    private static Configuration configuration;

    private Configuration readConfiguration() throws BenchmarkException {
        try {
            Ini ini = new Ini(new File(getClass().getClassLoader().getResource(Constants.CONFIG).getFile()));
            Preferences prefs = new IniPreferences(ini);
            configuration = new Configuration(prefs);

        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error reading Configuration File");
        }
        return null;
    }

    private void runBenchmark(BaseSchema schemaCreator, BaseDataUploader dataUploader, BaseQueryManager queryManager) {

        try {
            System.out.println("---------------------------------------------------------------\n");
            System.out.println(String.format("Running Benchmark On %s With Mapping %s",
                    schemaCreator.getDatabase().getName(), schemaCreator.getMapping()));

            // Creating schema on a particular database and particular mapping
            System.out.println("Creating Schema ...");
            schemaCreator.createSchema();

            // Inserting data into the database system after schema creation
            System.out.println("Inserting Data ...");
            Map<Integer, Duration> runTimePerMapping = new HashMap<Integer, Duration>();
            runTimePerMapping.put(0, dataUploader.addAllData());

            // Running benchmark queries and gathering query runtimes
            System.out.println("Running Queries ...");
            runTimePerMapping.putAll(queryManager.runQueries());

            runTimes.put(new Pair<>(queryManager.getDatabase(), queryManager.getMapping()), runTimePerMapping);

            // Cleaning up inserted data and dropping created schema
            System.out.println("Cleaning Up Database, Removing Data And Schema ...\n");
            schemaCreator.dropSchema();

            System.out.println("---------------------------------------------------------------\n");

        } catch (Exception | Error be) {
            be.printStackTrace();
            runTimes.put(new Pair<>(queryManager.getDatabase(), queryManager.getMapping()), null);
            try {
                System.out.println("Cleaning Up Database, Removing Data And Schema ...\n");
                schemaCreator.dropSchema();
            } catch (Exception | Error e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String args[]) {

        System.out.println("****Starting IoT Benchmark v0.1****\n");

        Benchmark benchmark = new Benchmark();

        try {

            System.out.println("Reading Configuration File benchmark.ini .... \n");
            benchmark.readConfiguration();

            System.out.println("Generating Test Data And Queries\n");
            Scale scaler = new Scale(configuration.isScaleQueries(), configuration.isScaleData());
            scaler.scaleDataAndQueries();

            if (true)
                return;

            System.out.println("Starting Up Database Servers\n");
            DBMSManager dbmsManager = new DBMSManager(configuration.getScriptsDir());
            //dbmsManager.startServers();

            for (Database database: configuration.getDatabases()) {
                switch (database) {
                    case GRIDDB:
                        configuration.getMappings().get(Database.GRIDDB).forEach(
                                e -> benchmark.runBenchmark(
                                            new GridDBSchema(e, configuration.getDataDir() + ROWS),
                                            new GridDBDataUploader(e, configuration.getDataDir() + ROWS),
                                            new GridDBQueryManager(e, configuration.getQueriesDir(),
                                                    configuration.isWriteOutput(), configuration.getTimeout())));
                        break;
                    case CRATEDB:
                        configuration.getMappings().get(Database.CRATEDB).forEach(
                                e -> benchmark.runBenchmark(
                                        new CrateDBSchema(e, configuration.getDataDir() + ROWS),
                                        new CrateDBDataUploader(e, configuration.getDataDir() + ROWS),
                                        new CrateDBQueryManager(e, configuration.getQueriesDir(),
                                                configuration.isWriteOutput(), configuration.getTimeout())));
                        break;
                    case MONGODB:
                        configuration.getMappings().get(Database.MONGODB).forEach(
                                e -> benchmark.runBenchmark(
                                        new MongoDBSchema(e, configuration.getDataDir() + MIX),
                                        new MongoDBDataUploader(e, configuration.getDataDir() + MIX),
                                        new MongoDBQueryManager(e, configuration.getQueriesDir(),
                                                configuration.isWriteOutput(), configuration.getTimeout())));
                        break;
                    case ASTERIXDB:
                        configuration.getMappings().get(Database.ASTERIXDB).forEach(
                                e -> benchmark.runBenchmark(
                                        new AsterixDBSchema(e, configuration.getDataDir() + OBJECTS),
                                        new AsterixDBDataUploader(e, configuration.getDataDir() + OBJECTS),
                                        new AsterixDBQueryManager(e, configuration.getQueriesDir(),
                                                configuration.isWriteOutput(), configuration.getTimeout())));
                        break;
                    case CASSANDRA:
                        configuration.getMappings().get(Database.CASSANDRA).forEach(
                                e -> benchmark.runBenchmark(
                                        new CassandraSchema(e, configuration.getDataDir() + ROWS),
                                        new CassandraDataUploader(e, configuration.getDataDir() + ROWS),
                                        new CassandraQueryManager(e, configuration.getQueriesDir(),
                                                configuration.isWriteOutput(), configuration.getTimeout())));
                        break;
                    case POSTGRESQL:
                        configuration.getMappings().get(Database.POSTGRESQL).forEach(
                                e -> benchmark.runBenchmark(
                                        new PgSQLSchema(e, configuration.getDataDir() + ROWS),
                                        new PgSQLDataUploader(e, configuration.getDataDir() + ROWS),
                                        new PgSQLQueryManager(e, configuration.getQueriesDir(),
                                                configuration.isWriteOutput(), configuration.getTimeout())));
                        break;
                    default:
                        throw new BenchmarkException("Database Not Supported");
                }
            }

            ReportBuilder builder = new ReportBuilder(runTimes, configuration.getReportsDir(),
                    configuration.getFormat());
            builder.createReport();
            System.out.println("\n****Report Written To Reports Directory****");

            System.out.println("Stopping All Database Servers");
            //dbmsManager.stopServers();
        } catch (BenchmarkException e) {
            e.printStackTrace();
        }
    }

}
