package edu.uci.ics.tippers.query.cratedb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.cratedb.CrateDBConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.query.postgresql.PgSQLQueryManager;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CrateDBQueryManager extends BaseQueryManager {

    private PgSQLQueryManager externalQueryManager;
    private Connection connection;

    public CrateDBQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        connection = CrateDBConnectionManager.getInstance().getConnection();
        externalQueryManager = new PgSQLQueryManager(mapping, queriesDir, outputDir, writeOutput, timeout, connection);
    }

    @Override
    public Database getDatabase() {
        return Database.CRATEDB;
    }

    @Override
    public void cleanUp() {

    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                return externalQueryManager.runQuery1(sensorId);
            default:
                throw new BenchmarkException("Error Running Query 1 on CrateDB");
        }
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                String query = "SELECT sen.name FROM SENSOR sen, SENSOR_TYPE st, " +
                        "COVERAGE_INFRASTRUCTURE ci WHERE sen.SENSOR_TYPE_ID=st.id AND st.name=? " +
                        "AND sen.id=ci.SENSOR_ID AND ci.INFRASTRUCTURE_ID=ANY(?)";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString(1, sensorTypeName);

                    Array locationsArray = connection.createArrayOf("string", locationIds.toArray());
                    stmt.setArray(2, locationsArray);
                    return externalQueryManager.runTimedQuery(stmt, 2);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                return externalQueryManager.runQuery3(sensorId, startTime, endTime);
            default:
                throw new BenchmarkException("Error Running Query 3 on CrateDB");
        }
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "SELECT timeStamp, payload FROM OBSERVATION WHERE timestamp>? AND timestamp<? " +
                        "AND SENSOR_ID = ANY(?)";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                    Array sensorIdArray = connection.createArrayOf("string", sensorIds.toArray());
                    stmt.setArray(3, sensorIdArray);

                    return externalQueryManager.runTimedQuery(stmt, 4);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT ID, SENSOR_TYPE_ID FROM SENSOR WHERE ID=ANY(?)";
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);
                    Array sensorIdArray = connection.createArrayOf("string", sensorIds.toArray());
                    stmt.setArray(1, sensorIdArray);

                    ResultSet rs = stmt.executeQuery();
                    String typeId = null;
                    List<String> wifiSensors = new ArrayList<>();
                    List<String> wemoSensors = new ArrayList<>();
                    List<String> thermoSensors = new ArrayList<>();

                    while(rs.next()) {
                        typeId = rs.getString(2);
                        if ("Thermometer".equals(typeId))
                            thermoSensors.add(rs.getString(1));
                        else if ("WeMo".equals(typeId))
                            wemoSensors.add(rs.getString(1));
                        else if ("WiFiAP".equals(typeId))
                            wifiSensors.add(rs.getString(1));
                    }
                    rs.close();

                    if (!thermoSensors.isEmpty()) {
                        query = "SELECT timeStamp, temperature FROM ThermometerObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=ANY(?)";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("string", thermoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        externalQueryManager.runTimedQuery(stmt, 4);
                    }
                    else if ("WeMo".equals(typeId)) {
                        query = "SELECT timeStamp, currentMilliWatts, onTodaySeconds FROM WeMoObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=ANY(?)";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("string", wemoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        externalQueryManager.runTimedQuery(stmt, 4);
                    }
                    else if ("WiFiAP".equals(typeId)) {
                        query = "SELECT timeStamp, clientId FROM WiFiAPObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=ANY(?)";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("string", wifiSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        externalQueryManager.runTimedQuery(stmt, 4);
                    }

                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (SQLException e) {
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
            case 2:
                return externalQueryManager.runQuery5(sensorTypeName, startTime, endTime, payloadAttribute,
                        startPayloadValue, endPayloadValue);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "SELECT obs.sensor_id, avg(counts) FROM " +
                        "(SELECT sensor_id, date_trunc('day', timestamp), " +
                        "count(*) as counts " +
                        "FROM OBSERVATION WHERE timestamp>? AND timestamp<? " +
                        "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                        "AS obs GROUP BY sensor_id";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                    Array sensorIdArray = connection.createArrayOf("string", sensorIds.toArray());
                    stmt.setArray(3, sensorIdArray);

                    return externalQueryManager.runTimedQuery(stmt, 6);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT ID, SENSOR_TYPE_ID FROM SENSOR WHERE ID=ANY(?)";
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);
                    Array sensorIdArray = connection.createArrayOf("string", sensorIds.toArray());
                    stmt.setArray(1, sensorIdArray);

                    ResultSet rs = stmt.executeQuery();
                    String typeId = null;
                    List<String> wifiSensors = new ArrayList<>();
                    List<String> wemoSensors = new ArrayList<>();
                    List<String> thermoSensors = new ArrayList<>();

                    while(rs.next()) {
                        typeId = rs.getString(2);
                        if ("Thermometer".equals(typeId))
                            thermoSensors.add(rs.getString(1));
                        else if ("WeMo".equals(typeId))
                            wemoSensors.add(rs.getString(1));
                        else if ("WiFiAP".equals(typeId))
                            wifiSensors.add(rs.getString(1));
                    }
                    rs.close();

                    if (!thermoSensors.isEmpty()) {
                        query = "SELECT obs.sensor_id, avg(counts) FROM " +
                                "(SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM ThermometerObservation WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("string", thermoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        externalQueryManager.runTimedQuery(stmt, 6);
                    }
                    else if ("WeMo".equals(typeId)) {
                        query = "SELECT obs.sensor_id, avg(counts) FROM " +
                                "(SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM WeMoObservation WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("string", wemoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        externalQueryManager.runTimedQuery(stmt, 6);
                    }
                    else if ("WiFiAP".equals(typeId)) {
                        query = "SELECT obs.sensor_id, avg(counts) FROM " +
                                "(SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM WiFiAPObservation WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("string", wifiSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        externalQueryManager.runTimedQuery(stmt, 6);
                    }

                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery8(String userId, Date date) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery9(String userId, String infraTypeName) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery10(Date startTime, Date endTime) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }
}
