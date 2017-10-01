package edu.uci.ics.tippers.query.griddb;

import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.griddb.StoreManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.execution.Benchmark;
import edu.uci.ics.tippers.operators.GroupBy;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static edu.uci.ics.tippers.common.util.Helper.getFileFromQuery;

public class GridDBQueryManager extends BaseQueryManager {

    private GridStore gridStore;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public GridDBQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        gridStore = StoreManager.getInstance().getGridStore();
        //sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    public Database getDatabase() {
        return Database.GRIDDB;
    }

    @Override
    public void cleanUp(){
        try {
            gridStore.close();
            gridStore = StoreManager.getInstance().getGridStore();
        } catch (GSException e) {
        }

    }

    private Duration runTimedQuery(String containerName, String query, int queryNum) throws BenchmarkException {
        try {
            Instant startTime = Instant.now();
            Container<String, Row> container = gridStore.getContainer(containerName);
            ContainerInfo containerInfo;
            Query<Row> gridDBQuery = container.query(query);
            RowSet<Row> rows = gridDBQuery.fetch();

            Row row;
            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(queryNum));
            while (rows.hasNext()) {
                row = rows.next();
                if (writeOutput) {
                    StringBuilder line = new StringBuilder("");
                    containerInfo = row.getSchema();
                    int columnCount = containerInfo.getColumnCount();
                    for (int i = 0; i < columnCount; i++) {
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

    private Row getById(String containerName, String id) throws BenchmarkException {
        try {
            Container<String, Row> container = gridStore.getContainer(containerName);
            return container.get(id);

        } catch (GSException ge) {
            ge.printStackTrace();
            throw new BenchmarkException("Error Running Query On GridDB");
        }
    }

    private List<Row> runQueryWithRows(String containerName, String query) throws BenchmarkException {
        try {
            Container<String, Row> container = gridStore.getContainer(containerName);
            Query<Row> gridDBQuery = container.query(query);
            RowSet<Row> rows = gridDBQuery.fetch();

            List<Row> rowList = new ArrayList<>();
            while (rows.hasNext()) {
                rowList.add(rows.next());
            }
            return rowList;
        } catch (GSException ge) {
            ge.printStackTrace();
            throw new BenchmarkException("Error Running Query On GridDB");
        }
    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        return runTimedQuery("Sensor", String.format("SELECT * FROM Sensor WHERE id='%s'",sensorId), 1);
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        Instant startTime = Instant.now();
        List<Row> sensorTypes = runQueryWithRows("SensorType",
                String.format("SELECT * FROM SensorType WHERE name='%s'", sensorTypeName));

        try {
            List<Row> sensors = runQueryWithRows("Sensor",
                    String.format("SELECT * FROM Sensor WHERE typeId='%s'", sensorTypes.get(0).getString(0)));
            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(2));
            for(Row row : sensors) {
                String coverageId = row.getString(6);
                Row coverage = getById("SensorCoverage", coverageId);
                String[] entitiesCovered = coverage.getStringArray(2);

                locationIds.forEach(e-> {
                    for (String s : entitiesCovered) {
                        if (s.equals(e)) {
                            if (writeOutput) {
                                try {
                                    writer.writeString(row.getString(0));
                                } catch (IOException e1) {
                                    e1.printStackTrace();
                                    throw new BenchmarkException("Error Running Query On GridDB");
                                }
                            }
                        }
                    }
                });
            }
            writer.close();
            Instant endTime = Instant.now();
            return Duration.between(startTime, endTime);
        } catch (IOException e) {
            throw new BenchmarkException("Error running query on griddb");        }
    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        String collectionName = Constants.GRIDDB_OBS_PREFIX + sensorId;
        String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                "AND timeStamp < TIMESTAMP('%s')", collectionName, sdf.format(startTime), sdf.format(endTime));
        return runTimedQuery(collectionName, query, 3);
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {

        Instant start = Instant.now();
        try {
            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(4));

            for (String sensorId : sensorIds) {
                String collectionName = Constants.GRIDDB_OBS_PREFIX + sensorId;
                String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                        "AND timeStamp < TIMESTAMP('%s')", collectionName, sdf.format(startTime), sdf.format(endTime));
                List<Row> observations = runQueryWithRows(collectionName, query);


                observations.forEach(e -> {
                    if (writeOutput) {
                        ContainerInfo containerInfo = null;
                        try {
                            StringBuilder line = new StringBuilder("");
                            containerInfo = e.getSchema();
                            int columnCount = containerInfo.getColumnCount();
                            for (int i = 0; i < columnCount; i++) {
                                line.append(e.getValue(i)).append("\t");
                            }
                            writer.writeString(line.toString());
                        } catch (IOException e1) {
                            e1.printStackTrace();
                            throw new BenchmarkException("Error Running Query On GridDB");
                        }
                    }
                });
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

        Instant end = Instant.now();
        return Duration.between(start, end);

    }

    @Override
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        Instant start = Instant.now();
        List<Row> sensorTypes = runQueryWithRows("SensorType",
                String.format("SELECT * FROM SensorType WHERE name='%s'", sensorTypeName));

        try {
            List<Row> sensors = runQueryWithRows("Sensor",
                    String.format("SELECT * FROM Sensor WHERE typeId='%s'", sensorTypes.get(0).getString(0)));

            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(5));
            for (Row row : sensors) {
                String collectionName = Constants.GRIDDB_OBS_PREFIX + row.getString(0);
                String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                        "AND timeStamp < TIMESTAMP('%s') AND %s >= %s AND %s <= %s ",
                        collectionName, sdf.format(startTime), sdf.format(endTime), payloadAttribute, startPayloadValue,
                        payloadAttribute, endPayloadValue);
                List<Row> observations = runQueryWithRows(collectionName, query);

                observations.forEach(e->{
                    if (writeOutput) {
                        ContainerInfo containerInfo = null;
                        try {
                            StringBuilder line = new StringBuilder("");
                            containerInfo = e.getSchema();
                            int columnCount = containerInfo.getColumnCount();
                            for (int i = 0; i < columnCount; i++) {
                                line.append(e.getValue(i)).append("\t");
                            }
                            writer.writeString(line.toString());
                        } catch (IOException e1) {
                            e1.printStackTrace();
                            throw new BenchmarkException("Error Running Query On GridDB");
                        }
                    }
                });

            }
            writer.close();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (IOException ge) {
            ge.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        Instant start = Instant.now();
        try {
            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(6));
            for (String sensorId : sensorIds) {
                String collectionName = Constants.GRIDDB_OBS_PREFIX + sensorId;
                String query = String.format("SELECT * FROM %s WHERE timeStamp >= TIMESTAMP('%s') " +
                        "AND timeStamp <= TIMESTAMP('%s')", collectionName, sdf.format(startTime), sdf.format(endTime));
                List<Row> observations = runQueryWithRows(collectionName, query);

                JSONArray jsonObservations = new JSONArray();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

                observations.forEach(e -> {
                    JSONObject object = new JSONObject();
                    try {
                        object.put("date", dateFormat.format(e.getTimestamp(0)));
                        jsonObservations.put(object);
                    } catch (GSException e1) {
                        e1.printStackTrace();
                    }
                });

                GroupBy groupBy = new GroupBy();
                JSONArray groups = groupBy.doGroupBy(jsonObservations, Arrays.asList("date"));
                final int[] sum = {0};

                groups.iterator().forEachRemaining(e-> {
                    sum[0] += ((JSONArray)e).length();
                });

                if (writeOutput) {
                    writer.writeString(sensorId + ", " + sum[0] /groups.length());
                }
            }
            writer.close();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }
}
