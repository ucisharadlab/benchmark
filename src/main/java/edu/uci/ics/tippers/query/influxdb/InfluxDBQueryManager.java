package edu.uci.ics.tippers.query.influxdb;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.Helper;
import edu.uci.ics.tippers.connection.influxdb.InfluxDBConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.operators.GroupBy;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

import static edu.uci.ics.tippers.common.util.Helper.getFileFromQuery;

public class InfluxDBQueryManager extends BaseQueryManager {

    private Connection metadataConnection;
    private InfluxDBConnectionManager connectionManager;
    private static final Logger LOGGER = Logger.getLogger(InfluxDBQueryManager.class);

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");


    public InfluxDBQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        connectionManager = InfluxDBConnectionManager.getInstance();
        metadataConnection = InfluxDBConnectionManager.getInstance().getMetadataConnection();
    }

    @Override
    public Database getDatabase() {
        return Database.INFLUXDB;
    }

    @Override
    public void cleanUp() {

    }

    private Duration runTimedQuery (String query, int queryNum) throws BenchmarkException {

        LOGGER.info(String.format("Running Query %s", queryNum));
        LOGGER.info(query);

        Instant startTime = Instant.now();
        HttpResponse response = connectionManager.sendQuery(query);
        Instant endTime = Instant.now();


        if (writeOutput) {
            // TODO: Write To File
            try {
                RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping,
                        Helper.getFileFromQuery(queryNum));
                writer.writeString(EntityUtils.toString(response.getEntity()));
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new BenchmarkException("Error writing output to file");
            }
        }
        return Duration.between(startTime, endTime);
    }

    private Duration explainQuery(String query, int queryNum) throws BenchmarkException {

        LOGGER.info(String.format("Running Query %s", queryNum));
        LOGGER.info(query);

        Instant startTime = Instant.now();
        HttpResponse response = connectionManager.sendQuery("EXPLAIN " + query);
        Instant endTime = Instant.now();

        try {
            RowWriter<String> writer = new RowWriter<>(outputDir+"/explains/", getDatabase(), mapping,
                    Helper.getFileFromQuery(queryNum));
            writer.writeString(EntityUtils.toString(response.getEntity()));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error writing output to file");
        }

        return Duration.between(startTime, endTime);
    }


    private Duration runTimedMetadataQuery(PreparedStatement stmt, int queryNum) throws BenchmarkException {
        try {
            Instant start = Instant.now();
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(queryNum));
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

    private JSONArray runQueryWithRows(String query) throws BenchmarkException {

        HttpResponse response = connectionManager.sendQuery(query);
        try {
            return new JSONObject(EntityUtils.toString(response.getEntity())).getJSONArray("values");
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error writing output to file");
        }
    }

    private List<List<Object>> runMetadataQueryWithRows(String query) throws BenchmarkException {

        try {
            PreparedStatement stmt = metadataConnection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            List<List<Object>> rows = new ArrayList<>();
            while(rs.next()) {
                List<Object> row = new ArrayList<>();
                for(int i = 1; i <= columnsNumber; i++)
                    row.add(rs.getObject(i));
                rows.add(row);
            }

            rs.close();

            return rows;
        } catch (SQLException  e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        String query = "SELECT name FROM SENSOR WHERE id=?";
        try {
            PreparedStatement stmt = metadataConnection.prepareStatement(query);
            stmt.setString(1, sensorId);
            return runTimedMetadataQuery(stmt, 1);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        String query = "SELECT sen.name FROM SENSOR sen, SENSOR_TYPE st, " +
                "COVERAGE_INFRASTRUCTURE ci WHERE sen.SENSOR_TYPE_ID=st.id AND st.name=? " +
                "AND sen.id=ci.SENSOR_ID AND ci.INFRASTRUCTURE_ID=ANY(?)";
        try {
            PreparedStatement stmt = metadataConnection.prepareStatement(query);
            stmt.setString(1, sensorTypeName);

            Array locationsArray = metadataConnection.createArrayOf("VARCHAR", locationIds.toArray());
            stmt.setArray(2, locationsArray);
            return runTimedMetadataQuery(stmt, 2);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 2:
                try {
                    List<List<Object>> sensorTypes = runMetadataQueryWithRows(
                            String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId));
                    String collectionName = sensorTypes.get(0).get(4) + "Observation";

                    String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                                    "AND timeStamp < TIMESTAMP('%s') AND sensorId='%s'",
                            collectionName, sdf.format(startTime), sdf.format(endTime), sensorId);
                    return runTimedQuery(query, 3);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            default:
                throw  new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {

            case 2:
                try {
                    Instant start = Instant.now();
                    String typeId = null;
                    List<String> wifiSensors = new ArrayList<>();
                    List<String> wemoSensors = new ArrayList<>();
                    List<String> thermoSensors = new ArrayList<>();

                    for (String sensorId : sensorIds) {
                        typeId = (String) runMetadataQueryWithRows(
                                String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId)).get(0).get(2);
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
                        runTimedQuery(query, 4);

                    }
                    if (!wemoSensors.isEmpty()) {
                        String query = String.format("SELECT * FROM WeMoObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                "AND timeStamp < TIMESTAMP('%s') AND ( "
                                + wemoSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                                + ");", sdf.format(startTime), sdf.format(endTime));
                        runTimedQuery(query, 4);

                    }
                    if (!wifiSensors.isEmpty()) {
                        String query = String.format("SELECT * FROM WiFiAPObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                "AND timeStamp < TIMESTAMP('%s') AND ( "
                                + wifiSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                                + ");", sdf.format(startTime), sdf.format(endTime));
                        runTimedQuery(query, 4);

                    }
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
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        switch (mapping) {

            case 2:
                Instant start = Instant.now();
                try {

                    String collectionName = sensorTypeName + "Observation";

                    String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                                    "AND timeStamp < TIMESTAMP('%s') AND %s >= %s AND %s <= %s ",
                            collectionName, sdf.format(startTime), sdf.format(endTime), payloadAttribute, startPayloadValue,
                            payloadAttribute, endPayloadValue);
                    runTimedQuery(query, 5);

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
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {

            case 2:
                Instant start = Instant.now();
                try {
                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(6));
                    JSONArray observations = null;

                    for (String sensorId : sensorIds) {
                        String typeId = (String) runMetadataQueryWithRows(
                                String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId)).get(0).get(2);

                        if ("Thermometer".equals(typeId)) {
                            String query = String.format("SELECT * FROM ThermometerObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                            "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            observations = runQueryWithRows(query);
                        }
                        else if ("WeMo".equals(typeId)){
                            String query = String.format("SELECT * FROM WeMoObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                            "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            observations = runQueryWithRows(query);
                        }
                        else if ("WiFiAP".equals(typeId)){
                            String query = String.format("SELECT * FROM WiFiAPObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                            "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            observations = runQueryWithRows(query);
                        }

                        JSONArray jsonObservations = new JSONArray();
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

                        observations.forEach(e -> {
                            JSONObject object = new JSONObject();
                            try {
                                object.put("date", dateFormat.format(((JSONArray)e).getString(1)));
                                jsonObservations.put(object);
                            } catch (Exception e1) {
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
            case 2:
                Instant start = Instant.now();
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                try {
                    Map<String, String> userMap = runMetadataQueryWithRows("SELECT * FROM Users")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return (String)e.get(0);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return (String)e.get(2);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(7));


                    JSONArray rows = runQueryWithRows(
                            String.format("SELECT * FROM Presence WHERE timeStamp >= TIMESTAMP('%s') " +
                                            "AND timeStamp <= TIMESTAMP('%s') AND location = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), startLocation));

                    for (Object row : rows) {

                        String query = String.format("SELECT * FROM Presence WHERE timeStamp >= TIMESTAMP('%s') " +
                                        "AND timeStamp <= TIMESTAMP('%s') AND location = '%s' AND semanticEntityId = '%s'",
                                sdf.format(((JSONArray)row).getString(1)), sdf.format(endTime), endLocation, ((JSONArray)row).getString(4));
                        JSONArray observations = runQueryWithRows(query);

                        observations.forEach(e->{
                            if (writeOutput) {
                                try {
                                    writer.writeString(userMap.get(((JSONArray)e).getString(4)));
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
            case 2:
                Instant start = Instant.now();
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date  endTime = cal.getTime();

                try {
                    Map<String, String> userMap = runMetadataQueryWithRows(
                            "SELECT * FROM Users")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return (String)e.get(0);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return (String)e.get(2);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(8));

                    JSONArray results = runQueryWithRows(String.format("SELECT * FROM Presence WHERE semanticEntityId = '%s' " +
                                    "AND timeStamp >= TIMESTAMP('%s') AND timeStamp <= TIMESTAMP('%s')",
                            userId, sdf.format(startTime), sdf.format(endTime)));
                    Iterator<Object> rows = results.iterator();

                    while (rows.hasNext()) {

                        JSONArray row = (JSONArray) rows.next();

                        String query = String.format("SELECT * FROM Presence WHERE timeStamp = TIMESTAMP('%s') " +
                                        "AND location='%s' AND semanticEntityId != '%s'", sdf.format(row.getString(1)),
                                row.getString(3), userId);
                        JSONArray observations = runQueryWithRows(query);

                        observations.forEach(e->{
                            if (writeOutput) {
                                try {
                                    writer.writeString(userMap.get(((JSONArray)e).getString(4)) + ", " +((JSONArray)e).getString(3));
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
            case 2:
                Instant start = Instant.now();

                try {
                    String infraTypeId = (String) runMetadataQueryWithRows(
                            String.format("SELECT * FROM Infrastructure_Type WHERE name='%s'", infraTypeName)).get(0).get(0);

                    List<String> infras = runMetadataQueryWithRows(
                            String.format("SELECT * FROM Infrastructure WHERE typeId='%s'", infraTypeId))
                            .stream().map(e -> {
                                try {
                                    return (String)e.get(0);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(9));
                    String query = String.format("SELECT * FROM Presence WHERE semanticEntityId='%s'", userId);
                    JSONArray observations = runQueryWithRows(query);

                    JSONArray jsonObservations = new JSONArray();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

                    observations.forEach(e -> {
                        JSONObject object = new JSONObject();
                        try {
                            if (infras.contains(((JSONArray)e).getString(3))) {
                                object.put("date", dateFormat.format(((JSONArray)e).getString(1)));
                                jsonObservations.put(object);
                            }
                        } catch (Exception e1) {
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
            case 2:
                Instant start = Instant.now();
                try {
                    Map<String, String> infraMap = runMetadataQueryWithRows("SELECT * FROM Infrastructure")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return (String)e.get(0);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return (String)e.get(1);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(10));
                    String query = String.format("SELECT * FROM Occupancy WHERE timeStamp >= TIMESTAMP('%s') " +
                                    "AND timeStamp <= TIMESTAMP('%s') ORDER BY semanticEntityId, timeStamp ",
                            sdf.format(startTime), sdf.format(endTime));
                    JSONArray observations = runQueryWithRows(query);

                    observations.forEach(e -> {
                        if (writeOutput) {
                            try {
                                StringBuilder line = new StringBuilder("");
                                int columnCount = ((JSONArray)e).length();
                                for (int i = 0; i < columnCount; i++) {
                                    if (i==4) line.append(infraMap.get(((JSONArray)e).getString(i))).append("\t");
                                    else line.append(((JSONArray)e).getString(i)).append("\t");
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
        String query = "SELECT name FROM SENSOR WHERE id=?";
        try {
            PreparedStatement stmt = metadataConnection.prepareStatement(query);
            stmt.setString(1, sensorId);
            return runTimedMetadataQuery(stmt, 1);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }

    @Override
    public Duration explainQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        String query = "SELECT sen.name FROM SENSOR sen, SENSOR_TYPE st, " +
                "COVERAGE_INFRASTRUCTURE ci WHERE sen.SENSOR_TYPE_ID=st.id AND st.name=? " +
                "AND sen.id=ci.SENSOR_ID AND ci.INFRASTRUCTURE_ID=ANY(?)";
        try {
            PreparedStatement stmt = metadataConnection.prepareStatement(query);
            stmt.setString(1, sensorTypeName);

            Array locationsArray = metadataConnection.createArrayOf("VARCHAR", locationIds.toArray());
            stmt.setArray(2, locationsArray);
            return runTimedMetadataQuery(stmt, 2);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }

    @Override
    public Duration explainQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 2:
                try {
                    List<List<Object>> sensorTypes = runMetadataQueryWithRows(
                            String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId));
                    String collectionName = sensorTypes.get(0).get(4) + "Observation";

                    String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                                    "AND timeStamp < TIMESTAMP('%s') AND sensorId='%s'",
                            collectionName, sdf.format(startTime), sdf.format(endTime), sensorId);
                    return explainQuery(query, 3);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            default:
                throw  new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration explainQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {

            case 2:
                try {
                    Instant start = Instant.now();
                    String typeId = null;
                    List<String> wifiSensors = new ArrayList<>();
                    List<String> wemoSensors = new ArrayList<>();
                    List<String> thermoSensors = new ArrayList<>();

                    for (String sensorId : sensorIds) {
                        typeId = (String) runMetadataQueryWithRows(
                                String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId)).get(0).get(2);
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
                        explainQuery(query, 4);

                    }
                    if (!wemoSensors.isEmpty()) {
                        String query = String.format("SELECT * FROM WeMoObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                "AND timeStamp < TIMESTAMP('%s') AND ( "
                                + wemoSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                                + ");", sdf.format(startTime), sdf.format(endTime));
                        explainQuery(query, 4);

                    }
                    if (!wifiSensors.isEmpty()) {
                        String query = String.format("SELECT * FROM WiFiAPObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                "AND timeStamp < TIMESTAMP('%s') AND ( "
                                + wifiSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                                + ");", sdf.format(startTime), sdf.format(endTime));
                        explainQuery(query, 4);

                    }
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
    public Duration explainQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        switch (mapping) {

            case 2:
                Instant start = Instant.now();
                try {

                    String collectionName = sensorTypeName + "Observation";

                    String query = String.format("SELECT * FROM %s WHERE timeStamp > TIMESTAMP('%s') " +
                                    "AND timeStamp < TIMESTAMP('%s') AND %s >= %s AND %s <= %s ",
                            collectionName, sdf.format(startTime), sdf.format(endTime), payloadAttribute, startPayloadValue,
                            payloadAttribute, endPayloadValue);
                    explainQuery(query, 5);

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

            case 2:
                Instant start = Instant.now();
                try {
                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(6));
                    JSONArray observations = null;

                    for (String sensorId : sensorIds) {
                        String typeId = (String) runMetadataQueryWithRows(
                                String.format("SELECT * FROM Sensor WHERE id='%s'", sensorId)).get(0).get(2);

                        if ("Thermometer".equals(typeId)) {
                            String query = String.format("SELECT * FROM ThermometerObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                            "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            observations = runQueryWithRows(query);
                        }
                        else if ("WeMo".equals(typeId)){
                            String query = String.format("SELECT * FROM WeMoObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                            "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            observations = runQueryWithRows(query);
                        }
                        else if ("WiFiAP".equals(typeId)){
                            String query = String.format("SELECT * FROM WiFiAPObservation WHERE timeStamp > TIMESTAMP('%s') " +
                                            "AND timeStamp < TIMESTAMP('%s') AND sensorId = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), sensorId);
                            observations = runQueryWithRows(query);
                        }

                        JSONArray jsonObservations = new JSONArray();
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

                        observations.forEach(e -> {
                            JSONObject object = new JSONObject();
                            try {
                                object.put("date", dateFormat.format(((JSONArray)e).getString(1)));
                                jsonObservations.put(object);
                            } catch (Exception e1) {
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
    public Duration explainQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 2:
                Instant start = Instant.now();
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                try {
                    Map<String, String> userMap = runMetadataQueryWithRows("SELECT * FROM Users")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return (String)e.get(0);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return (String)e.get(2);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(7));


                    JSONArray rows = runQueryWithRows(
                            String.format("SELECT * FROM Presence WHERE timeStamp >= TIMESTAMP('%s') " +
                                            "AND timeStamp <= TIMESTAMP('%s') AND location = '%s'",
                                    sdf.format(startTime), sdf.format(endTime), startLocation));

                    for (Object row : rows) {

                        String query = String.format("SELECT * FROM Presence WHERE timeStamp >= TIMESTAMP('%s') " +
                                        "AND timeStamp <= TIMESTAMP('%s') AND location = '%s' AND semanticEntityId = '%s'",
                                sdf.format(((JSONArray)row).getString(1)), sdf.format(endTime), endLocation, ((JSONArray)row).getString(4));
                        JSONArray observations = runQueryWithRows(query);

                        observations.forEach(e->{
                            if (writeOutput) {
                                try {
                                    writer.writeString(userMap.get(((JSONArray)e).getString(4)));
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
    public Duration explainQuery8(String userId, Date date) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 2:
                Instant start = Instant.now();
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date  endTime = cal.getTime();

                try {
                    Map<String, String> userMap = runMetadataQueryWithRows(
                            "SELECT * FROM Users")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return (String)e.get(0);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return (String)e.get(2);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(8));

                    JSONArray results = runQueryWithRows(String.format("SELECT * FROM Presence WHERE semanticEntityId = '%s' " +
                                    "AND timeStamp >= TIMESTAMP('%s') AND timeStamp <= TIMESTAMP('%s')",
                            userId, sdf.format(startTime), sdf.format(endTime)));
                    Iterator<Object> rows = results.iterator();

                    while (rows.hasNext()) {

                        JSONArray row = (JSONArray) rows.next();

                        String query = String.format("SELECT * FROM Presence WHERE timeStamp = TIMESTAMP('%s') " +
                                        "AND location='%s' AND semanticEntityId != '%s'", sdf.format(row.getString(1)),
                                row.getString(3), userId);
                        JSONArray observations = runQueryWithRows(query);

                        observations.forEach(e->{
                            if (writeOutput) {
                                try {
                                    writer.writeString(userMap.get(((JSONArray)e).getString(4)) + ", " +((JSONArray)e).getString(3));
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
    public Duration explainQuery9(String userId, String infraTypeName) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 2:
                Instant start = Instant.now();

                try {
                    String infraTypeId = (String) runMetadataQueryWithRows(
                            String.format("SELECT * FROM Infrastructure_Type WHERE name='%s'", infraTypeName)).get(0).get(0);

                    List<String> infras = runMetadataQueryWithRows(
                            String.format("SELECT * FROM Infrastructure WHERE typeId='%s'", infraTypeId))
                            .stream().map(e -> {
                                try {
                                    return (String)e.get(0);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }).collect(Collectors.toList());

                    RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(9));
                    String query = String.format("SELECT * FROM Presence WHERE semanticEntityId='%s'", userId);
                    JSONArray observations = runQueryWithRows(query);

                    JSONArray jsonObservations = new JSONArray();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

                    observations.forEach(e -> {
                        JSONObject object = new JSONObject();
                        try {
                            if (infras.contains(((JSONArray)e).getString(3))) {
                                object.put("date", dateFormat.format(((JSONArray)e).getString(1)));
                                jsonObservations.put(object);
                            }
                        } catch (Exception e1) {
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
    public Duration explainQuery10(Date startTime, Date endTime) throws BenchmarkException {
        // TODO: Fix Error Due to TimeZone
        switch (mapping) {
            case 2:
                Instant start = Instant.now();
                try {
                    Map<String, String> infraMap = runMetadataQueryWithRows("SELECT * FROM Infrastructure")
                            .stream().collect(Collectors.toMap(e -> {
                                try {
                                    return (String)e.get(0);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }, e -> {
                                try {
                                    return (String)e.get(1);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                    return null;
                                }
                            }));

                    String query = String.format("SELECT * FROM Occupancy WHERE time >= '%s' " +
                                    "AND time <= '%s' ORDER BY time ",
                            sdf.format(startTime), sdf.format(endTime));
                    explainQuery(query, 10);

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
