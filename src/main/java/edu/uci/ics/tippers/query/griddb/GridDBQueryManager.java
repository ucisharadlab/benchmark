package edu.uci.ics.tippers.query.griddb;

import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.griddb.StoreManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
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
import java.util.stream.Collectors;

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

    private Duration explainQuery(String containerName, String query, int queryNum) throws BenchmarkException {
        try {
            Instant startTime = Instant.now();
            Container<String, Row> container = gridStore.getContainer(containerName);
            ContainerInfo containerInfo;
            Query<QueryAnalysisEntry> gridDBQuery = container.query(query, QueryAnalysisEntry.class);
            RowSet<QueryAnalysisEntry> rows = gridDBQuery.fetch();

            QueryAnalysisEntry row;
            RowWriter<String> writer = new RowWriter<>(outputDir+"/explains/", getDatabase(), mapping, getFileFromQuery(queryNum));
            while (rows.hasNext()) {
                row = rows.next();
                StringBuilder line = new StringBuilder("");
                line.append(row.getDepth()).append(",\t")
                .append(row.getId()).append(",\t")
                .append(row.getStatement()).append(",\t")
                .append(row.getType()).append(",\t")
                .append(row.getValue()).append(",\t")
                .append(row.getValueType());
                writer.writeString(line.toString());
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
        switch (mapping) {
            case 1:
            case 2:
                return runTimedQuery("Sensor", String.format("SELECT * FROM Sensor WHERE id='%s'",sensorId), 1);
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                Instant startTime = Instant.now();
                List<Row> sensorTypes = runQueryWithRows("SensorType",
                        String.format("SELECT * FROM SensorType WHERE name='%s'", sensorTypeName));

                try {
                    List<Row> sensors = runQueryWithRows("Sensor",
                            String.format("SELECT * FROM Sensor WHERE typeId='%s'", sensorTypes.get(0).getString(0)));
                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(2));
                    for(Row row : sensors) {
                        String[] entitiesCovered = row.getStringArray(5);

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
                    throw new BenchmarkException("Error running query on griddb");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String collectionName = Constants.GRIDDB_OBS_PREFIX + sensorId;
                String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                        "AND timeStamp < TIMESTAMP('%s')", collectionName, sdf.format(startTime), sdf.format(endTime));
                return runTimedQuery(collectionName, query, 3);
            case 2:
                try {
                    List<Row> sensorTypes = runQueryWithRows("Sensor",
                            String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId));
                    collectionName = sensorTypes.get(0).getString(2) + "Observation";
                    query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                            "AND timeStamp < TIMESTAMP('%s') AND sensorId='%s'",
                            collectionName, sdf.format(startTime), sdf.format(endTime), sensorId);
                    return runTimedQuery(collectionName, query, 3);
                } catch (GSException e) {
                    e.printStackTrace();
                }
            default:
                throw  new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
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
            case 2:
                try {
                    start = Instant.now();
                    String typeId = null;
                    List<String> wifiSensors = new ArrayList<>();
                    List<String> wemoSensors = new ArrayList<>();
                    List<String> thermoSensors = new ArrayList<>();

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(4));
                    for (String sensorId : sensorIds) {
                        typeId = runQueryWithRows("Sensor",
                                String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId)).get(0).getString(2);
                        if ("Thermometer".equals(typeId))
                            thermoSensors.add(sensorId);
                        else if ("WeMo".equals(typeId))
                            wemoSensors.add(sensorId);
                        else if ("WiFiAP".equals(typeId))
                            wifiSensors.add(sensorId);
                    }

                    if (!thermoSensors.isEmpty()) {
                        String query = String.format("SELECT * FROM ThermometerObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                "AND timeStamp < TIMESTAMP('%s') AND ( "
                                + thermoSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                                + ");", sdf.format(startTime), sdf.format(endTime));
                        List<Row> observations = runQueryWithRows("ThermometerObservation", query);

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
                    if (!wemoSensors.isEmpty()) {
                        String query = String.format("SELECT * FROM WeMoObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                "AND timeStamp < TIMESTAMP('%s') AND ( "
                                + wemoSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                                + ");", sdf.format(startTime), sdf.format(endTime));
                        List<Row> observations = runQueryWithRows("WeMoObservation", query);

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
                    if (!wifiSensors.isEmpty()) {
                        String query = String.format("SELECT * FROM WiFiAPObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                "AND timeStamp < TIMESTAMP('%s') AND ( "
                                + wifiSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                                + ");", sdf.format(startTime), sdf.format(endTime));
                        List<Row> observations = runQueryWithRows("WiFiAPObservation", query);


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
                    end = Instant.now();
                    return Duration.between(start, end);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        switch (mapping) {
            case 1:
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
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (IOException ge) {
                    ge.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();
                try {

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(5));
                    String collectionName = sensorTypeName + "Observation";

                    String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                                    "AND timeStamp < TIMESTAMP('%s') AND %s >= %s AND %s <= %s ",
                            collectionName, sdf.format(startTime), sdf.format(endTime), payloadAttribute, startPayloadValue,
                            payloadAttribute, endPayloadValue);
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


                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (IOException ge) {
                    ge.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No such mapping");
        }
    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 1:
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

                        groups.iterator().forEachRemaining(e -> {
                            sum[0] += ((JSONArray) e).length();
                        });

                        if (writeOutput) {
                            if (groups.length() != 0)
                                writer.writeString(sensorId + ", " + sum[0] / groups.length());
                        }
                    }
                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();
                try {
                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(6));
                    List<Row> observations = null;

                    for (String sensorId : sensorIds) {
                        String typeId = runQueryWithRows("Sensor",
                                String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId)).get(0).getString(2);

                        if ("Thermometer".equals(typeId)) {
                            String query = String.format("SELECT * FROM ThermometerObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                    "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            observations = runQueryWithRows("ThermometerObservation", query);
                        }
                        else if ("WeMo".equals(typeId)){
                            String query = String.format("SELECT * FROM WeMoObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                    "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            observations = runQueryWithRows("WeMoObservation", query);
                        }
                        else if ("WiFiAP".equals(typeId)){
                            String query = String.format("SELECT * FROM WiFiAPObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                    "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            observations = runQueryWithRows("WiFiAPObservation", query);
                        }

                        JSONArray jsonObservations = new JSONArray();
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

                        observations.forEach(e -> {
                            JSONObject object = new JSONObject();
                            try {
                                object.put("date", dateFormat.format(e.getTimestamp(1)));
                                jsonObservations.put(object);
                            } catch (GSException e1) {
                                e1.printStackTrace();
                            }
                        });

                        GroupBy groupBy = new GroupBy();
                        JSONArray groups = groupBy.doGroupBy(jsonObservations, Arrays.asList("date"));
                        final int[] sum = {0};

                            groups.iterator().forEachRemaining(e -> {
                            sum[0] += ((JSONArray) e).length();
                        });

                        if (writeOutput) {
                            if (groups.length() != 0)
                                writer.writeString(sensorId + ", " + sum[0] / groups.length());
                        }
                    }
                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No such mapping");
        }
    }

    @Override
    public Duration runQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 1:
                Instant start = Instant.now();
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                try {
                    List<String[]> users = runQueryWithRows("User",
                            "SELECT * FROM User")
                            .stream().map(e -> {
                                try {
                                    return new String[]{e.getString(0), e.getString(2)};
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(7));

                    for (String[] user : users) {
                        String collectionName = Constants.GRIDDB_SO_PREFIX + user[0];
                        List<Row> rows = runQueryWithRows(collectionName, String.format("SELECT * FROM %s WHERE timeStamp >= TIMESTAMP('%s') " +
                                        "AND timeStamp <= TIMESTAMP('%s') AND location = '%s'",
                                collectionName, sdf.format(startTime), sdf.format(endTime), startLocation));
                        for (Row row : rows) {

                            String query = String.format("SELECT * FROM %s WHERE timeStamp >= TIMESTAMP('%s') " +
                                            "AND timeStamp <= TIMESTAMP('%s') AND location = '%s'", collectionName,
                                    sdf.format(row.getTimestamp(0)), sdf.format(endTime), endLocation);
                            List<Row> observations = runQueryWithRows(collectionName, query);

                            if (observations.size() > 0 && writeOutput) {
                                writer.writeString(user[1]);
                            }
                        }
                    }
                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();
                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();

                try {
                    Map<String, String> userMap = runQueryWithRows("User",
                            "SELECT * FROM User")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return e.getString(2);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(7));


                    List<Row> rows = runQueryWithRows("Presence",
                            String.format("SELECT * FROM Presence WHERE timeStamp >= TIMESTAMP('%s') " +
                                    "AND timeStamp <= TIMESTAMP('%s') AND location = '%s'",
                            sdf.format(startTime), sdf.format(endTime), startLocation));
                    for (Row row : rows) {

                        String query = String.format("SELECT * FROM Presence WHERE timeStamp >= TIMESTAMP('%s') " +
                                        "AND timeStamp <= TIMESTAMP('%s') AND location = '%s' AND semanticEntityId = '%s'",
                                sdf.format(row.getTimestamp(1)), sdf.format(endTime), endLocation, row.getString(4));
                        List<Row> observations = runQueryWithRows("Presence", query);

                        observations.forEach(e->{
                            if (writeOutput) {
                                try {
                                    writer.writeString(userMap.get(e.getString(4)));
                                } catch (IOException e1) {
                                    e1.printStackTrace();
                                }

                            }
                        });
                    }

                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery8(String userId, Date date) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 1:
                Instant start = Instant.now();
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                try {
                    List<String[]> users = runQueryWithRows("User",
                            String.format("SELECT * FROM User WHERE id != '%s'", userId))
                            .stream().map(e -> {
                                try {
                                    return new String[]{e.getString(0), e.getString(2)};
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(8));

                    Container<String, Row> container = gridStore.getContainer(Constants.GRIDDB_SO_PREFIX + userId);
                    Query<Row> gridDBQuery = container.query(String.format("SELECT * FROM %s WHERE timeStamp >= TIMESTAMP('%s') " +
                                    "AND timeStamp <= TIMESTAMP('%s')",
                            Constants.GRIDDB_SO_PREFIX + userId, sdf.format(startTime), sdf.format(endTime)));
                    RowSet<Row> rows = gridDBQuery.fetch();

                    while (rows.hasNext()) {

                        Row row = rows.next();
                        for (String[] user : users) {
                            String collectionName = Constants.GRIDDB_SO_PREFIX + user[0];
                            String query = String.format("SELECT * FROM %s WHERE timeStamp = TIMESTAMP('%s') " +
                                    "AND location='%s'", collectionName, sdf.format(row.getTimestamp(0)), row.getString(3));
                            List<Row> observations = runQueryWithRows(collectionName, query);

                            if (observations.size() > 0 && writeOutput) {
                                writer.writeString(user[1] + ", " + row.getString(3));
                            }
                        }
                    }
                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();
                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();

                try {
                    Map<String, String> userMap = runQueryWithRows("User",
                            "SELECT * FROM User")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return e.getString(2);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(8));

                    Container<String, Row> container = gridStore.getContainer("Presence");
                    Query<Row> gridDBQuery = container.query(String.format("SELECT * FROM Presence WHERE semanticEntityId = '%s' " +
                                    "AND timeStamp >= TIMESTAMP('%s') AND timeStamp <= TIMESTAMP('%s')",
                            userId, sdf.format(startTime), sdf.format(endTime)));
                    RowSet<Row> rows = gridDBQuery.fetch();

                    while (rows.hasNext()) {

                        Row row = rows.next();

                        String query = String.format("SELECT * FROM Presence WHERE timeStamp = TIMESTAMP('%s') " +
                                "AND location='%s' AND semanticEntityId != '%s'", sdf.format(row.getTimestamp(1)),
                                row.getString(3), userId);
                        List<Row> observations = runQueryWithRows("Presence", query);

                        observations.forEach(e->{
                            if (writeOutput) {
                                try {
                                    writer.writeString(userMap.get(e.getString(4)) + ", " +e.getString(3));
                                } catch (IOException e1) {
                                    e1.printStackTrace();
                                }

                            }
                        });
                    }
                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No such mapping");
        }
    }

    @Override
    public Duration runQuery9(String userId, String infraTypeName) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                try {
                    String infraTypeId = runQueryWithRows("InfrastructureType",
                            String.format("SELECT * FROM InfrastructureType WHERE name='%s'", infraTypeName)).get(0).getString(0);

                    List<String> infras = runQueryWithRows("Infrastructure",
                            String.format("SELECT * FROM Infrastructure WHERE typeId='%s'", infraTypeId))
                            .stream().map(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(9));
                    String collectionName = Constants.GRIDDB_SO_PREFIX + userId;
                    String query = String.format("SELECT * FROM %s", collectionName);
                    List<Row> observations = runQueryWithRows(collectionName, query);

                    JSONArray jsonObservations = new JSONArray();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

                    observations.forEach(e -> {
                        JSONObject object = new JSONObject();
                        try {
                            if (infras.contains(e.getString(3))) {
                                object.put("date", dateFormat.format(e.getTimestamp(0)));
                                jsonObservations.put(object);
                            }
                        } catch (GSException e1) {
                            e1.printStackTrace();
                        }
                    });

                    GroupBy groupBy = new GroupBy();
                    JSONArray groups = groupBy.doGroupBy(jsonObservations, Arrays.asList("date"));
                    final int[] sum = {0};

                    groups.iterator().forEachRemaining(e -> {
                        sum[0] += ((JSONArray) e).length();
                    });

                    if (writeOutput) {
                        if (groups.length() != 0)
                            writer.writeString(userId + ", " + sum[0] * 10 / groups.length());
                    }

                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();

                try {
                    String infraTypeId = runQueryWithRows("InfrastructureType",
                            String.format("SELECT * FROM InfrastructureType WHERE name='%s'", infraTypeName)).get(0).getString(0);

                    List<String> infras = runQueryWithRows("Infrastructure",
                            String.format("SELECT * FROM Infrastructure WHERE typeId='%s'", infraTypeId))
                            .stream().map(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(9));
                    String query = String.format("SELECT * FROM Presence WHERE semanticEntityId='%s'", userId);
                    List<Row> observations = runQueryWithRows("Presence", query);

                    JSONArray jsonObservations = new JSONArray();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

                    observations.forEach(e -> {
                        JSONObject object = new JSONObject();
                        try {
                            if (infras.contains(e.getString(3))) {
                                object.put("date", dateFormat.format(e.getTimestamp(1)));
                                jsonObservations.put(object);
                            }
                        } catch (GSException e1) {
                            e1.printStackTrace();
                        }
                    });

                    GroupBy groupBy = new GroupBy();
                    JSONArray groups = groupBy.doGroupBy(jsonObservations, Arrays.asList("date"));
                    final int[] sum = {0};

                    groups.iterator().forEachRemaining(e -> {
                        sum[0] += ((JSONArray) e).length();
                    });

                    if (writeOutput) {
                        if (groups.length() != 0)
                            writer.writeString(userId + ", " + sum[0] * 10 / groups.length());
                    }

                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery10(Date startTime, Date endTime) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 1:
                Instant start = Instant.now();
                try {
                    List<String> infras = runQueryWithRows("Infrastructure",
                            "SELECT * FROM Infrastructure")
                            .stream().map(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(10));
                    for (String infra : infras) {
                        String collectionName = Constants.GRIDDB_SO_PREFIX + infra;
                        String query = String.format("SELECT * FROM %s WHERE timeStamp >= TIMESTAMP('%s') " +
                                "AND timeStamp <= TIMESTAMP('%s') ORDER BY timeStamp", collectionName, sdf.format(startTime), sdf.format(endTime));
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
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();
                try {
                    Map<String, String> infraMap = runQueryWithRows("Infrastructure",
                            "SELECT * FROM Infrastructure")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return e.getString(1);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(10));
                    String query = String.format("SELECT * FROM Occupancy WHERE timeStamp >= TIMESTAMP('%s') " +
                            "AND timeStamp <= TIMESTAMP('%s') ORDER BY semanticEntityId, timeStamp ",
                            sdf.format(startTime), sdf.format(endTime));
                    List<Row> observations = runQueryWithRows("Occupancy", query);

                    observations.forEach(e -> {
                        if (writeOutput) {
                            ContainerInfo containerInfo = null;
                            try {
                                StringBuilder line = new StringBuilder("");
                                containerInfo = e.getSchema();
                                int columnCount = containerInfo.getColumnCount();
                                for (int i = 0; i < columnCount; i++) {
                                    if (i==4) line.append(infraMap.get(e.getValue(i))).append("\t");
                                    else line.append(e.getValue(i)).append("\t");
                                }
                                writer.writeString(line.toString());
                            } catch (IOException e1) {
                                e1.printStackTrace();
                                throw new BenchmarkException("Error Running Query On GridDB");
                            }
                        }
                    });

                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                return explainQuery("Sensor", String.format("EXPLAIN ANALYZE SELECT * FROM Sensor WHERE id='%s'",sensorId), 1);
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }


    @Override
    public Duration explainQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                Instant startTime = Instant.now();
                List<Row> sensorTypes = runQueryWithRows("SensorType",
                        String.format("SELECT * FROM SensorType WHERE name='%s'", sensorTypeName));

                try {
                    explainQuery("Sensor",
                            String.format("EXPLAIN ANALYZE SELECT * FROM Sensor WHERE typeId='%s'", sensorTypes.get(0).getString(0)),2);

                    Instant endTime = Instant.now();
                    return Duration.between(startTime, endTime);
                } catch (IOException e) {
                    throw new BenchmarkException("Error running query on griddb");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration explainQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String collectionName = Constants.GRIDDB_OBS_PREFIX + sensorId;
                String query = String.format("EXPLAIN SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                        "AND timeStamp < TIMESTAMP('%s')", collectionName, sdf.format(startTime), sdf.format(endTime));
                return explainQuery(collectionName, query, 3);
            case 2:
                try {
                    List<Row> sensorTypes = runQueryWithRows("Sensor",
                            String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId));
                    collectionName = sensorTypes.get(0).getString(2) + "Observation";
                    query = String.format("EXPLAIN ANALYZE SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                                    "AND timeStamp < TIMESTAMP('%s') AND sensorId='%s'",
                            collectionName, sdf.format(startTime), sdf.format(endTime), sensorId);
                    return explainQuery(collectionName, query, 3);
                } catch (GSException e) {
                    e.printStackTrace();
                }
            default:
                throw  new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration explainQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
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
            case 2:
                try {
                    start = Instant.now();
                    String typeId = null;
                    List<String> wifiSensors = new ArrayList<>();
                    List<String> wemoSensors = new ArrayList<>();
                    List<String> thermoSensors = new ArrayList<>();

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(4));
                    for (String sensorId : sensorIds) {
                        typeId = runQueryWithRows("Sensor",
                                String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId)).get(0).getString(2);
                        if ("Thermometer".equals(typeId))
                            thermoSensors.add(sensorId);
                        else if ("WeMo".equals(typeId))
                            wemoSensors.add(sensorId);
                        else if ("WiFiAP".equals(typeId))
                            wifiSensors.add(sensorId);
                    }

                    if (!thermoSensors.isEmpty()) {
                        String query = String.format("EXPLAIN ANALYZE SELECT * FROM ThermometerObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                "AND timeStamp < TIMESTAMP('%s') AND ( "
                                + thermoSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                                + ");", sdf.format(startTime), sdf.format(endTime));
                        explainQuery("ThermometerObservation", query, 4);

                    }
                    if (!wemoSensors.isEmpty()) {
                        String query = String.format("EXPLAIN  ANALYZE SELECT * FROM WeMoObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                "AND timeStamp < TIMESTAMP('%s') AND ( "
                                + wemoSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                                + ");", sdf.format(startTime), sdf.format(endTime));
                        explainQuery("WeMoObservation", query, 4);

                    }
                    if (!wifiSensors.isEmpty()) {
                        String query = String.format("EXPLAIN  ANALYZE SELECT * FROM WiFiAPObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                "AND timeStamp < TIMESTAMP('%s') AND ( "
                                + wifiSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                                + ");", sdf.format(startTime), sdf.format(endTime));
                        explainQuery("WiFiAPObservation", query, 4);
                    }
                    writer.close();
                    end = Instant.now();
                    return Duration.between(start, end);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration explainQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        switch (mapping) {
            case 1:
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
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (IOException ge) {
                    ge.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();
                try {

                    String collectionName = sensorTypeName + "Observation";

                    String query = String.format("EXPLAIN ANALYZE SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                                    "AND timeStamp < TIMESTAMP('%s') AND %s >= %s AND %s <= %s ",
                            collectionName, sdf.format(startTime), sdf.format(endTime), payloadAttribute, startPayloadValue,
                            payloadAttribute, endPayloadValue);
                    explainQuery(collectionName, query, 5);

                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception ge) {
                    ge.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No such mapping");
        }
    }

    @Override
    public Duration explainQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 1:
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

                        groups.iterator().forEachRemaining(e -> {
                            sum[0] += ((JSONArray) e).length();
                        });

                        if (writeOutput) {
                            if (groups.length() != 0)
                                writer.writeString(sensorId + ", " + sum[0] / groups.length());
                        }
                    }
                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();
                try {
                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(6));
                    List<Row> observations = null;

                    for (String sensorId : sensorIds) {
                        String typeId = runQueryWithRows("Sensor",
                                String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId)).get(0).getString(2);

                        if ("Thermometer".equals(typeId)) {
                            String query = String.format("EXPLAIN ANALYZE SELECT * FROM ThermometerObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                            "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            explainQuery("ThermometerObservation", query, 6);
                        }
                        else if ("WeMo".equals(typeId)){
                            String query = String.format("EXPLAIN ANALYZE SELECT * FROM WeMoObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                            "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            explainQuery("WeMoObservation", query, 6);
                        }
                        else if ("WiFiAP".equals(typeId)){
                            String query = String.format("EXPLAIN ANALYZE SELECT * FROM WiFiAPObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                            "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            explainQuery("WiFiAPObservation", query, 6);
                        }
                        break;
                    }
                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No such mapping");
        }
    }

    @Override
    public Duration explainQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 1:
                Instant start = Instant.now();
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                try {
                    List<String[]> users = runQueryWithRows("User",
                            "SELECT * FROM User")
                            .stream().map(e -> {
                                try {
                                    return new String[]{e.getString(0), e.getString(2)};
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(7));

                    for (String[] user : users) {
                        String collectionName = Constants.GRIDDB_SO_PREFIX + user[0];
                        List<Row> rows = runQueryWithRows(collectionName, String.format("SELECT * FROM %s WHERE timeStamp >= TIMESTAMP('%s') " +
                                        "AND timeStamp <= TIMESTAMP('%s') AND location = '%s'",
                                collectionName, sdf.format(startTime), sdf.format(endTime), startLocation));
                        for (Row row : rows) {

                            String query = String.format("SELECT * FROM %s WHERE timeStamp >= TIMESTAMP('%s') " +
                                            "AND timeStamp <= TIMESTAMP('%s') AND location = '%s'", collectionName,
                                    sdf.format(row.getTimestamp(0)), sdf.format(endTime), endLocation);
                            List<Row> observations = runQueryWithRows(collectionName, query);

                            if (observations.size() > 0 && writeOutput) {
                                writer.writeString(user[1]);
                            }
                        }
                    }
                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();
                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();

                try {
                    Map<String, String> userMap = runQueryWithRows("User",
                            "SELECT * FROM User")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return e.getString(2);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(7));


                    explainQuery("Presence",
                            String.format("EXPLAIN ANALYZE SELECT * FROM Presence WHERE timeStamp >= TIMESTAMP('%s') " +
                                            "AND timeStamp <= TIMESTAMP('%s') AND location = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), startLocation), 7);


                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery8(String userId, Date date) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 1:
                Instant start = Instant.now();
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                try {
                    List<String[]> users = runQueryWithRows("User",
                            String.format("SELECT * FROM User WHERE id != '%s'", userId))
                            .stream().map(e -> {
                                try {
                                    return new String[]{e.getString(0), e.getString(2)};
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(8));

                    explainQuery(Constants.GRIDDB_SO_PREFIX + userId,
                            String.format("EXPLAIN ANALYZE SELECT * FROM %s WHERE timeStamp >= TIMESTAMP('%s') " +
                                    "AND timeStamp <= TIMESTAMP('%s')",
                            Constants.GRIDDB_SO_PREFIX + userId, sdf.format(startTime), sdf.format(endTime)), 8);

                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();
                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();

                try {
                    Map<String, String> userMap = runQueryWithRows("User",
                            "SELECT * FROM User")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return e.getString(2);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(8));

                    explainQuery("Presence", String.format("EXPLAIN ANALYZE SELECT * FROM Presence WHERE semanticEntityId = '%s' " +
                                    "AND timeStamp >= TIMESTAMP('%s') AND timeStamp <= TIMESTAMP('%s')",
                            userId, sdf.format(startTime), sdf.format(endTime)), 8);


                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No such mapping");
        }
    }

    @Override
    public Duration explainQuery9(String userId, String infraTypeName) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                try {
                    String infraTypeId = runQueryWithRows("InfrastructureType",
                            String.format("SELECT * FROM InfrastructureType WHERE name='%s'", infraTypeName)).get(0).getString(0);

                    List<String> infras = runQueryWithRows("Infrastructure",
                            String.format("SELECT * FROM Infrastructure WHERE typeId='%s'", infraTypeId))
                            .stream().map(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(9));
                    String collectionName = Constants.GRIDDB_SO_PREFIX + userId;
                    String query = String.format("SELECT * FROM %s", collectionName);
                    List<Row> observations = runQueryWithRows(collectionName, query);

                    JSONArray jsonObservations = new JSONArray();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

                    observations.forEach(e -> {
                        JSONObject object = new JSONObject();
                        try {
                            if (infras.contains(e.getString(3))) {
                                object.put("date", dateFormat.format(e.getTimestamp(0)));
                                jsonObservations.put(object);
                            }
                        } catch (GSException e1) {
                            e1.printStackTrace();
                        }
                    });

                    GroupBy groupBy = new GroupBy();
                    JSONArray groups = groupBy.doGroupBy(jsonObservations, Arrays.asList("date"));
                    final int[] sum = {0};

                    groups.iterator().forEachRemaining(e -> {
                        sum[0] += ((JSONArray) e).length();
                    });

                    if (writeOutput) {
                        if (groups.length() != 0)
                            writer.writeString(userId + ", " + sum[0] * 10 / groups.length());
                    }

                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();

                try {
                    String infraTypeId = runQueryWithRows("InfrastructureType",
                            String.format("SELECT * FROM InfrastructureType WHERE name='%s'", infraTypeName)).get(0).getString(0);

                    List<String> infras = runQueryWithRows("Infrastructure",
                            String.format("SELECT * FROM Infrastructure WHERE typeId='%s'", infraTypeId))
                            .stream().map(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    String query = String.format("EXPLAIN ANALYZE SELECT * FROM Presence WHERE semanticEntityId='%s'", userId);
                    explainQuery("Presence", query, 9);


                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery10(Date startTime, Date endTime) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 1:
                Instant start = Instant.now();
                try {
                    List<String> infras = runQueryWithRows("Infrastructure",
                            "SELECT * FROM Infrastructure")
                            .stream().map(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir+"/explains/", getDatabase(), mapping, getFileFromQuery(10));
                    for (String infra : infras) {
                        String collectionName = Constants.GRIDDB_SO_PREFIX + infra;
                        String query = String.format("SELECT * FROM %s WHERE timeStamp >= TIMESTAMP('%s') " +
                                "AND timeStamp <= TIMESTAMP('%s') ORDER BY timeStamp", collectionName, sdf.format(startTime), sdf.format(endTime));
                        List<Row> observations = runQueryWithRows(collectionName, query);

                        observations.forEach(e -> {
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
                        });
                    }
                    writer.close();
                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                start = Instant.now();
                try {
                    Map<String, String> infraMap = runQueryWithRows("Infrastructure",
                            "SELECT * FROM Infrastructure")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return e.getString(0);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return e.getString(1);
                                } catch (GSException e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    String query = String.format("EXPLAIN ANALYZE SELECT * FROM Occupancy WHERE timeStamp >= TIMESTAMP('%s') " +
                                    "AND timeStamp <= TIMESTAMP('%s') ORDER BY semanticEntityId, timeStamp ",
                            sdf.format(startTime), sdf.format(endTime));
                    explainQuery("Occupancy", query, 10);

                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }
}
