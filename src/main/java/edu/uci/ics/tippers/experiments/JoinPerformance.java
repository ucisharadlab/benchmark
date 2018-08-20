package edu.uci.ics.tippers.experiments;

import com.toshiba.mwcloud.gs.*;
import com.toshiba.mwcloud.gs.TimeUnit;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.griddb.StoreManager;
import edu.uci.ics.tippers.connection.postgresql.PgSQLConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.execution.Configuration;
import edu.uci.ics.tippers.query.QueryCSVReader;
import edu.uci.ics.tippers.writer.RowWriter;
import org.ini4j.Ini;
import org.ini4j.IniPreferences;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.prefs.Preferences;

import static edu.uci.ics.tippers.common.util.Helper.getFileFromQuery;

public class JoinPerformance {

    private Connection pgConnection;
    private GridStore gridStore;
    private Configuration configuration;

    protected boolean writeOutput;
    protected String queriesDir;
    protected String outputDir;
    protected long timeout;

    public JoinPerformance() {
        pgConnection = PgSQLConnectionManager.getInstance().getConnection();
        gridStore = StoreManager.getInstance().getGridStore();

        configuration = readConfiguration();

        this.writeOutput = configuration.isWriteOutput();
        this.outputDir = configuration.getOutputDir();
        this.queriesDir = configuration.getQueriesDir();
        this.timeout = configuration.getTimeout();
    }

    private Duration runWithThread(Callable<Duration> query) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Duration> future = executorService.submit(query);
        try {
            return future.get(timeout, java.util.concurrent.TimeUnit.MILLISECONDS);
        }catch (TimeoutException e) {
            e.printStackTrace();
            throw new BenchmarkException("Query Timed Out");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        } finally {
            executorService.shutdown();
            try {
                executorService.awaitTermination(Constants.SHUTDOWN_WAIT, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cleanUp();
        }
    }

    private void cleanUp(){

    }

    public Duration runPostgreSQLTimedQuery(PreparedStatement stmt, int queryNum) throws BenchmarkException {
        try {
            Instant start = Instant.now();
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            RowWriter<String> writer = new RowWriter<>(outputDir, Database.POSTGRESQL, 3,
                    getFileFromQuery(queryNum));
            while(rs.next()) {
                if (writeOutput) {
                    StringBuilder line = new StringBuilder("");
                    for(int i = 1; i <= columnsNumber; i++)
                        line.append(rs.getString(i)).append("\t");
                    writer.writeString(line.toString());
                }
            }
            writer.close();
            rs.close();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }

    private Duration runGridDBTimedQuery(String containerName, String query, int queryNum) throws BenchmarkException {
        try {
            Instant startTime = Instant.now();
            Container<String, Row> container = gridStore.getContainer(containerName);
            ContainerInfo containerInfo;
            Query<Row> gridDBQuery = container.query(query);
            RowSet<Row> rows = gridDBQuery.fetch();

            Row row;
            RowWriter<String> writer = new RowWriter<>(outputDir, Database.GRIDDB, 3,
                    getFileFromQuery(queryNum));
            while (rows.hasNext()) {
                row = rows.next();
                if (writeOutput) {
                    StringBuilder line = new StringBuilder("");
                    containerInfo = row.getSchema();
                    int columnCount = containerInfo.getColumnCount();
                    for (int i = 0; i < columnCount; i++) {
                        if (containerInfo.getColumnInfo(i).getType().equals(GSType.STRING_ARRAY))
                            line.append(Arrays.toString(row.getStringArray(i))).append("\t");
                        else
                            line.append(row.getValue(i)).append("\t");
                    }
                    writer.writeString(line.toString());
                }
            }
            writer.close();
            Instant endTime = Instant.now();
            return Duration.between(startTime, endTime);

        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query On GridDB");
        }
    }

    public Duration runPGQuery(String sensorId) throws BenchmarkException {

        String query = "SELECT name FROM SENSOR WHERE id=?";
        try {
            PreparedStatement stmt = pgConnection.prepareStatement(query);
            stmt.setString(1, sensorId);
            return runPostgreSQLTimedQuery(stmt, 11);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

    }

    public Duration runGridDBQuery(String sensorId) throws BenchmarkException {

        return runGridDBTimedQuery("Sensor",
                String.format("SELECT * FROM Sensor WHERE id='%s'",sensorId), 11);

    }

    public List<Duration> runExperiment() {

        int numQueries = 0;
        Duration runTime = Duration.ZERO;
        String values[];
        List<Duration> queryRunTimes = new ArrayList<>();
        QueryCSVReader reader = new QueryCSVReader(queriesDir + getFileFromQuery(11));
        try {
            while ((values = reader.readNextLine()) != null) {
                runTime = runWithThread(()->runPGQuery("TODO"));
                numQueries++;
                queryRunTimes.add(runTime);
                System.out.println(String.format("Postgres Count: %s Time: %s", numQueries, runTime.toString()));
            }
        } catch (Exception | Error e) {
            e.printStackTrace();
            queryRunTimes.add(Constants.MAX_DURATION);
        }

        numQueries = 0;
        reader = new QueryCSVReader(queriesDir + getFileFromQuery(11));
        try {
            while ((values = reader.readNextLine()) != null) {
                runTime = runTime.plus(runWithThread(()->runGridDBQuery("TODO")));
                numQueries++;
                queryRunTimes.add(runTime);
                System.out.println(String.format("GridDB Count: %s Time: %s", numQueries, runTime.toString()));
            }
        } catch (Exception | Error e) {
            e.printStackTrace();
            queryRunTimes.add(Constants.MAX_DURATION);
        }
        return queryRunTimes;
    }

    private Configuration readConfiguration() throws BenchmarkException {
        try {
            Ini ini = new Ini(new File(getClass().getClassLoader().getResource(Constants.CONFIG).getFile()));
            Preferences prefs = new IniPreferences(ini);
            configuration = new Configuration(prefs);

        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error reading Configuration File");
        }
        return configuration;
    }


    public static void main(String args[]) {
        JoinPerformance exp = new JoinPerformance();
        List<Duration> results = exp.runExperiment();
        System.out.print(results);
    }

}
