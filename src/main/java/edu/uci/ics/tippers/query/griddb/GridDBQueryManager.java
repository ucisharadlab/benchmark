package edu.uci.ics.tippers.query.griddb;

import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.griddb.StoreManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GridDBQueryManager extends BaseQueryManager {

    private GridStore gridStore;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public GridDBQueryManager(int mapping, String queriesDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, writeOutput, timeout);
        gridStore = StoreManager.getInstance().getGridStore();
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

    private Duration runTimedQuery(String containerName, String query) throws BenchmarkException {
        try {
            Instant startTime = Instant.now();
            Container<String, Row> container = gridStore.getContainer(containerName);
            ContainerInfo containerInfo;
            Query<Row> gridDBQuery = container.query(query);
            RowSet<Row> rows = gridDBQuery.fetch();

            Row row;

            while (rows.hasNext()) {
                row = rows.next();
                if (writeOutput) {
                    // TODO: Write output to a file
                    containerInfo = row.getSchema();
                    int columnCount = containerInfo.getColumnCount();
                    for (int i = 0; i < columnCount; i++) {
                        System.out.println(row.getValue(i));
                    }
                }
            }
            Instant endTime = Instant.now();
            return Duration.between(startTime, endTime);

        } catch (GSException ge) {
            ge.printStackTrace();
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
        return runTimedQuery("Sensor", String.format("SELECT * FROM Sensor WHERE id='%s'",sensorId));
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        Instant startTime = Instant.now();
        List<Row> sensorTypes = runQueryWithRows("SensorType",
                String.format("SELECT * FROM SensorType WHERE name='%s'", sensorTypeName));

        try {
            List<Row> sensors = runQueryWithRows("Sensor",
                    String.format("SELECT * FROM Sensor WHERE typeId='%s'", sensorTypes.get(0).getString(0)));
            for(Row row : sensors) {
                String coverageId = row.getString(6);
                Row coverage = getById("SensorCoverage", coverageId);
                String[] entitiesCovered = coverage.getStringArray(2);
                locationIds.forEach(e-> {
                    for (String s : entitiesCovered) {
                        if (s.equals(e)) {
                            if (writeOutput) {
                                // TODO: Write output to a file
                                try {
                                    System.out.println(row.getValue(0));
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    throw new BenchmarkException("Error Running Query On GridDB");
                                }
                            }
                        }
                    }
                });
            }
            Instant endTime = Instant.now();
            return Duration.between(startTime, endTime);
        } catch (GSException ge) {
            throw new BenchmarkException("Error running query on griddb");
        }
    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        String collectionName = Constants.GRIDDB_OBS_PREFIX + sensorId;
        String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                "AND timeStamp < TIMESTAMP('%s')", collectionName, sdf.format(startTime), sdf.format(endTime));
        return runTimedQuery(collectionName, query);
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {

        Instant start = Instant.now();

        for (String sensorId : sensorIds) {
            String collectionName = Constants.GRIDDB_OBS_PREFIX + sensorId;
            String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                    "AND timeStamp < TIMESTAMP('%s')", collectionName, sdf.format(startTime), sdf.format(endTime));
            List<Row> observations = runQueryWithRows(collectionName, query);

            observations.forEach(e->{
                if (writeOutput) {
                    // TODO: Write output to a file
                    ContainerInfo containerInfo = null;
                    try {
                        containerInfo = e.getSchema();
                        int columnCount = containerInfo.getColumnCount();
                        for (int i = 0; i < columnCount; i++) {
                            System.out.println(e.getValue(i));
                        }
                    } catch (GSException e1) {
                        e1.printStackTrace();
                        throw new BenchmarkException("Error Running Query On GridDB");
                    }
                }
            });

        }
        Instant end = Instant.now();
        return Duration.between(start, end);

    }

    @Override
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        return null;
    }
}
