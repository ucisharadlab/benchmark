package edu.uci.ics.tippers.query.sparksql;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.sparksql.SparkSQLConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.query.postgresql.PgSQLQueryManager;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class SparkSQLQueryManager extends BaseQueryManager {

    private PgSQLQueryManager externalQueryManager;
    private Connection connection;

    public SparkSQLQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        connection = SparkSQLConnectionManager.getInstance().getConnection();
        externalQueryManager = new PgSQLQueryManager(mapping, queriesDir, outputDir, writeOutput, timeout, connection,
                Database.SPARKSQL);
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
                        "AND sen.id=ci.SENSOR_ID AND ( " +
                        locationIds.stream().map(e -> "ci.INFRASTRUCTURE_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString(1, sensorTypeName);

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
                String query = "SELECT timeStamp, payload FROM OBSERVATION WHERE timestamp>'?' AND timestamp<'?' " +
                        "AND SENSOR_ID=?";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, sensorId);

                    return externalQueryManager.runTimedQuery(stmt, 3);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT SENSOR_TYPE_ID FROM SENSOR WHERE ID=?";
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString(1, sensorId);
                    ResultSet rs = stmt.executeQuery();
                    String typeId = null;
                    while(rs.next()) {
                        typeId = rs.getString(1);
                    }
                    rs.close();

                    if ("Thermometer".equals(typeId))
                        query = "SELECT timeStamp, temperature FROM ThermometerObservation  WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND SENSOR_ID=?";
                    else if ("WeMo".equals(typeId))
                        query = "SELECT timeStamp, currentMilliWatts, onTodaySeconds FROM WeMoObservation  WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND SENSOR_ID=?";
                    else if ("WiFiAP".equals(typeId))
                        query = "SELECT timeStamp, clientId FROM WiFiAPObservation  WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND SENSOR_ID=?";

                    stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, sensorId);

                    externalQueryManager.runTimedQuery(stmt, 3);

                    Instant end = Instant.now();
                    return Duration.between(start, end);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("Error Running Query 3 on CrateDB");
        }
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "SELECT timeStamp, payload FROM OBSERVATION WHERE timestamp>'?' AND timestamp<'?' " +
                        "AND (" + sensorIds.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                    return externalQueryManager.runTimedQuery(stmt, 4);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT ID, SENSOR_TYPE_ID FROM SENSOR WHERE ( " +
                        sensorIds.stream().map(e -> "ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                ;
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);

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
                        query = "SELECT timeStamp, temperature FROM ThermometerObservation  WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND (" + thermoSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        externalQueryManager.runTimedQuery(stmt, 4);
                    }
                    else if (!wemoSensors.isEmpty()) {
                        query = "SELECT timeStamp, currentMilliWatts, onTodaySeconds FROM WeMoObservation  WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND (" + wemoSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        externalQueryManager.runTimedQuery(stmt, 4);
                    }
                    else if (!wifiSensors.isEmpty()) {
                        query = "SELECT timeStamp, clientId FROM WiFiAPObservation  WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND (" + wifiSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

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
                String query = String.format("SELECT timeStamp, payload FROM OBSERVATION o, SENSOR s, SENSOR_TYPE st  " +
                                "WHERE s.id = o.sensor_id AND s.sensor_type_id=st.id AND st.name=? AND " +
                                "timestamp>'?' AND timestamp<'?' AND payload['%s'] >= ? AND payload['%s'] <= ? ",
                        payloadAttribute, payloadAttribute);
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);

                    stmt.setString(1, sensorTypeName);
                    stmt.setTimestamp(2, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(3, new Timestamp(endTime.getTime()));
                    if (startPayloadValue instanceof  Integer) {
                        stmt.setInt(4, (Integer) startPayloadValue);
                        stmt.setInt(5, (Integer) endPayloadValue);
                    } else if (startPayloadValue instanceof  Double) {
                        stmt.setDouble(4, (Double) startPayloadValue);
                        stmt.setDouble(5, (Double) endPayloadValue);
                    }
                    return externalQueryManager.runTimedQuery(stmt, 5);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = String.format("SELECT * FROM %sOBSERVATION o " +
                                "WHERE timestamp>? AND timestamp<'?' AND %s>=? AND %s<=?", sensorTypeName,
                        payloadAttribute, payloadAttribute);
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);

                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    if (startPayloadValue instanceof  Integer) {
                        stmt.setInt(3, (Integer) startPayloadValue);
                        stmt.setInt(4, (Integer) endPayloadValue);
                    } else if (startPayloadValue instanceof  Double) {
                        stmt.setDouble(3, (Double) startPayloadValue);
                        stmt.setDouble(4, (Double) endPayloadValue);
                    }
                    return externalQueryManager.runTimedQuery(stmt, 5);
                } catch (SQLException  e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "SELECT obs.sensor_id, avg(counts) FROM " +
                        "( SELECT sensor_id, date_format( timestamp, 'yyyy-MM-dd'), " +
                        "count(*) as counts " +
                        "FROM OBSERVATION WHERE timestamp>'?' AND timestamp<'?' " +
                        "AND (" +
                        sensorIds.stream().map(e -> "ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ") " +
                        " GROUP BY sensor_id, date_format( timestamp, 'yyyy-MM-dd')) " +
                        "AS obs GROUP BY sensor_id";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                    return externalQueryManager.runTimedQuery(stmt, 6);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT ID, SENSOR_TYPE_ID FROM SENSOR WHERE (" +
                        sensorIds.stream().map(e -> "ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);

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
                                "( SELECT sensor_id, date_format( timestamp, 'yyyy-MM-dd'), " +
                                "count(*) as counts " +
                                "FROM ThermometerObservation WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND ( " +
                                thermoSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ") " +
                                " GROUP BY sensor_id, date_format( timestamp, 'yyyy-MM-dd')) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        externalQueryManager.runTimedQuery(stmt, 6);
                    }
                    else if (!wemoSensors.isEmpty()) {
                        query = "SELECT obs.sensor_id, avg(counts) FROM " +
                                "( SELECT sensor_id, date_format( timestamp, 'yyyy-MM-dd'), " +
                                "count(*) as counts " +
                                "FROM WeMoObservation WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND ( " +
                                thermoSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ") " +
                                " GROUP BY sensor_id, date_format( timestamp, 'yyyy-MM-dd')) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        externalQueryManager.runTimedQuery(stmt, 6);
                    }
                    else if (!wifiSensors.isEmpty()) {
                        query = "SELECT obs.sensor_id, avg(counts) FROM " +
                                "(SELECT sensor_id, date_format( timestamp, 'yyyy-MM-dd'), " +
                                "count(*) as counts " +
                                "FROM WiFiAPObservation WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND ( " +
                                thermoSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ") " +
                                " GROUP BY sensor_id, date_format( timestamp, 'yyyy-MM-dd')) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

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
        switch (mapping) {
            case 1:
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                String query = "SELECT u.name " +
                        "FROM SEMANTIC_OBSERVATION s1, SEMANTIC_OBSERVATION s2, SEMANTIC_OBSERVATION_TYPE st, USERS u " +
                        "WHERE s1.timeStamp >= ? AND s1.timeStamp <= ?  AND s2.timeStamp <= ? " +
                        "AND st.name = 'presence' AND st.id = s1.type_id AND st.id = s2.type_id " +
                        "AND s1.semantic_entity_id = s2.semantic_entity_id " +
                        "AND s1.payload['location'] = ? AND s2.payload['location'] = ? " +
                        "AND s1.timeStamp < s2.timeStamp " +
                        "AND s1.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp (2, new Timestamp(endTime.getTime()));
                    stmt.setTimestamp (3, new Timestamp(endTime.getTime()));
                    stmt.setString(4, startLocation);
                    stmt.setString(5, endLocation);

                    return externalQueryManager.runTimedQuery(stmt, 7);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();
                query = "SELECT u.name " +
                        "FROM PRESENCE s1, PRESENCE s2, USERS u " +
                        "WHERE s1.timeStamp >= ? AND s1.timeStamp <= ?  AND s2.timeStamp <= ? " +
                        "AND s1.semantic_entity_id = s2.semantic_entity_id " +
                        "AND s1.location = ? AND s2.location = ? " +
                        "AND s1.timeStamp < s2.timeStamp " +
                        "AND s1.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp (2, new Timestamp(endTime.getTime()));
                    stmt.setTimestamp (3, new Timestamp(endTime.getTime()));
                    stmt.setString(4, startLocation);
                    stmt.setString(5, endLocation);

                    return externalQueryManager.runTimedQuery(stmt, 7);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery8(String userId, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                String query = "SELECT u.name, s1.payload " +
                        "FROM SEMANTIC_OBSERVATION s1, SEMANTIC_OBSERVATION s2, SEMANTIC_OBSERVATION_TYPE st, USERS u " +
                        "WHERE s1.timeStamp >= '?' AND s1.timeStamp <= '?' " +
                        "AND s2.timeStamp = s1.timeStamp " +
                        "AND st.name = 'presence' AND s1.type_id = s2.type_id AND st.id = s1.type_id  " +
                        "AND s1.semantic_entity_id = ? AND s1.semantic_entity_id != s2.semantic_entity_id " +
                        "AND s1.payload['location'] = s2.payload['location'] AND s2.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp (2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, userId);

                    return externalQueryManager.runTimedQuery(stmt, 8);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }

            case 2:
                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();
                query = "SELECT u.name, s1.location " +
                        "FROM PRESENCE s1, PRESENCE s2, USERS u " +
                        "WHERE s1.timeStamp >= '?' AND s1.timeStamp <= '?' " +
                        "AND s2.timeStamp = s1.timeStamp " +
                        "AND s1.semantic_entity_id = ? AND s1.semantic_entity_id != s2.semantic_entity_id " +
                        "AND s2.semantic_entity_id = u.id AND s1.location = s2.location ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp (2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, userId);

                    return externalQueryManager.runTimedQuery(stmt, 8);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery8WithSelectivity(String userId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                String query = "SELECT u.name, s1.location " +
                        "FROM PRESENCE s1, PRESENCE s2, USERS u " +
                        "WHERE s1.timeStamp >= ? AND s1.timeStamp <= ? " +
                        "AND s2.timeStamp = s1.timeStamp " +
                        "AND s1.semantic_entity_id = ? AND s1.semantic_entity_id != s2.semantic_entity_id " +
                        "AND s2.semantic_entity_id = u.id AND s1.location = s2.location ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp (2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, userId);

                    return externalQueryManager.runTimedQuery(stmt, 8);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery9(String userId, String infraTypeName) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "SELECT Avg(timeSpent) as avgTimeSpent FROM " +
                        " (SELECT date_format( so.timestamp, 'yyyy-MM-dd'), count(*)*10 as timeSpent " +
                        "  FROM SEMANTIC_OBSERVATION so, Infrastructure infra, Infrastructure_Type infraType, SEMANTIC_OBSERVATION_TYPE st " +
                        "  WHERE st.name = 'presence' AND so.type_id = st.id " +
                        "  AND so.payload['location'] = infra.id " +
                        "  AND infra.INFRASTRUCTURE_TYPE_ID = infraType.id AND infraType.name = ? " +
                        "  AND so.semantic_entity_id = ? " +
                        "  GROUP BY  date_format( so.timestamp, 'yyyy-MM-dd')) AS timeSpentPerDay";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString (1, infraTypeName);
                    stmt.setString(2, userId);

                    return externalQueryManager.runTimedQuery(stmt, 9);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
            	query = "SELECT Avg(timeSpent) as avgTimeSpent FROM " +
                        " (SELECT  date_format( so.timestamp, 'yyyy-MM-dd'), count(*)*10 as timeSpent " +
                        "  FROM PRESENCE so, Infrastructure infra, Infrastructure_Type infraType " +
                        "  WHERE so.location = infra.id " +
                        "  AND infra.INFRASTRUCTURE_TYPE_ID = infraType.id AND infraType.name = ? " +
                        "  AND so.semantic_entity_id = ? " +
                        "  GROUP BY   date_format( so.timestamp, 'yyyy-MM-dd')) AS timeSpentPerDay";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString (1, infraTypeName);
                    stmt.setString(2, userId);

                    return externalQueryManager.runTimedQuery(stmt, 9);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery10(Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "SELECT infra.name, so.timeStamp, so.payload " +
                        "FROM SEMANTIC_OBSERVATION so, INFRASTRUCTURE infra, SEMANTIC_OBSERVATION_TYPE st " +
                        "WHERE so.timeStamp > '?' AND so.timeStamp < '?' " +
                        "AND so.type_id = 'occupancy' AND so.type_id = st.id AND so.semantic_entity_id = infra.id " +
                        "ORDER BY so.semantic_entity_id, so.timeStamp";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    return externalQueryManager.runTimedQuery(stmt, 10);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT infra.name, so.timeStamp, so.occupancy " +
                        "FROM OCCUPANCY so, INFRASTRUCTURE infra " +
                        "WHERE so.timeStamp > '?' AND so.timeStamp < '?' " +
                        "AND so.semantic_entity_id = infra.id " +
                        "ORDER BY so.semantic_entity_id, so.timeStamp";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    return externalQueryManager.runTimedQuery(stmt, 10);
                } catch (SQLException e) {
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
                return externalQueryManager.explainQuery1(sensorId);
            default:
                throw new BenchmarkException("Error Running Query 1 on CrateDB");
        }
    }

    @Override
    public Duration explainQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                String query = "EXPLAIN EXTENDED SELECT sen.name FROM SENSOR sen, SENSOR_TYPE st, " +
                        "COVERAGE_INFRASTRUCTURE ci WHERE sen.SENSOR_TYPE_ID=st.id AND st.name=? " +
                        "AND sen.id=ci.SENSOR_ID AND ( " +
                        locationIds.stream().map(e -> "ci.INFRASTRUCTURE_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString(1, sensorTypeName);

                    return externalQueryManager.explainQuery(stmt, 2);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                return externalQueryManager.explainQuery3(sensorId, startTime, endTime);
            default:
                throw new BenchmarkException("Error Running Query 3 on CrateDB");
        }
    }

    @Override
    public Duration explainQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "EXPLAIN EXTENDED SELECT timeStamp, payload FROM OBSERVATION WHERE timestamp>'?' AND timestamp<'?' " +
                        "AND (" + sensorIds.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                    return externalQueryManager.explainQuery(stmt, 4);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT ID, SENSOR_TYPE_ID FROM SENSOR WHERE ( " +
                        sensorIds.stream().map(e -> "ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                ;
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);

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
                        query = "EXPLAIN EXTENDED SELECT timeStamp, temperature FROM ThermometerObservation  WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND (" + thermoSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        externalQueryManager.explainQuery(stmt, 4);
                    }
                    else if (!wemoSensors.isEmpty()) {
                        query = "EXPLAIN EXTENDED SELECT timeStamp, currentMilliWatts, onTodaySeconds FROM WeMoObservation  WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND (" + wemoSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        externalQueryManager.explainQuery(stmt, 4);
                    }
                    else if (!wifiSensors.isEmpty()) {
                        query = "EXPLAIN EXTENDED SELECT timeStamp, clientId FROM WiFiAPObservation  WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND (" + wifiSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        externalQueryManager.explainQuery(stmt, 4);
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
    public Duration explainQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = String.format("EXPLAIN EXTENDED SELECT timeStamp, payload FROM OBSERVATION o, SENSOR s, SENSOR_TYPE st  " +
                                "WHERE s.id = o.sensor_id AND s.sensor_type_id=st.id AND st.name=? AND " +
                                "timestamp>'?' AND timestamp<'?' AND payload['%s'] >= ? AND payload['%s'] <= ? ",
                        payloadAttribute, payloadAttribute);
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);

                    stmt.setString(1, sensorTypeName);
                    stmt.setTimestamp(2, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(3, new Timestamp(endTime.getTime()));
                    if (startPayloadValue instanceof  Integer) {
                        stmt.setInt(4, (Integer) startPayloadValue);
                        stmt.setInt(5, (Integer) endPayloadValue);
                    } else if (startPayloadValue instanceof  Double) {
                        stmt.setDouble(4, (Double) startPayloadValue);
                        stmt.setDouble(5, (Double) endPayloadValue);
                    }
                    return externalQueryManager.explainQuery(stmt, 5);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                return externalQueryManager.explainQuery5(sensorTypeName, startTime, endTime, payloadAttribute,
                        startPayloadValue, endPayloadValue);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "EXPLAIN EXTENDED SELECT obs.sensor_id, avg(counts) FROM " +
                        "( SELECT sensor_id, date_trunc('day', timestamp), " +
                        "count(*) as counts " +
                        "FROM OBSERVATION WHERE timestamp>'?' AND timestamp<'?' " +
                        "AND (" +
                        sensorIds.stream().map(e -> "ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ") " +
                        " GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                        "AS obs GROUP BY sensor_id";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                    return externalQueryManager.explainQuery(stmt, 6);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT ID, SENSOR_TYPE_ID FROM SENSOR WHERE (" +
                        sensorIds.stream().map(e -> "ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ")";
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);

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
                        query = "EXPLAIN EXTENDED SELECT obs.sensor_id, avg(counts) FROM " +
                                "( SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM ThermometerObservation WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND ( " +
                                thermoSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ") " +
                                " GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        externalQueryManager.explainQuery(stmt, 6);
                    }
                    else if (!wemoSensors.isEmpty()) {
                        query = "EXPLAIN EXTENDED SELECT obs.sensor_id, avg(counts) FROM " +
                                "( SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM WeMoObservation WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND ( " +
                                thermoSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ") " +
                                " GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        externalQueryManager.explainQuery(stmt, 6);
                    }
                    else if (!wifiSensors.isEmpty()) {
                        query = "EXPLAIN EXTENDED SELECT obs.sensor_id, avg(counts) FROM " +
                                "(SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM WiFiAPObservation WHERE timestamp>'?' AND timestamp<'?' " +
                                "AND ( " +
                                thermoSensors.stream().map(e -> "SENSOR_ID='" + e + "'" ).collect(Collectors.joining(" OR ")) + ") " +
                                " GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        externalQueryManager.explainQuery(stmt, 6);
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
    public Duration explainQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                String query = "EXPLAIN EXTENDED SELECT u.name " +
                        "FROM SEMANTIC_OBSERVATION s1, SEMANTIC_OBSERVATION s2, SEMANTIC_OBSERVATION_TYPE st, USERS u " +
                        "WHERE s1.timeStamp >= ? AND s1.timeStamp <= ?  AND s2.timeStamp <= ? " +
                        "AND st.name = 'presence' AND st.id = s1.type_id AND st.id = s2.type_id " +
                        "AND s1.semantic_entity_id = s2.semantic_entity_id " +
                        "AND s1.payload['location'] = ? AND s2.payload['location'] = ? " +
                        "AND s1.timeStamp < s2.timeStamp " +
                        "AND s1.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp (2, new Timestamp(endTime.getTime()));
                    stmt.setTimestamp (3, new Timestamp(endTime.getTime()));
                    stmt.setString(4, startLocation);
                    stmt.setString(5, endLocation);

                    return externalQueryManager.explainQuery(stmt, 7);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();
                query = "EXPLAIN EXTENDED SELECT u.name " +
                        "FROM PRESENCE s1, PRESENCE s2, USERS u " +
                        "WHERE s1.timeStamp >= ? AND s1.timeStamp <= ?  AND s2.timeStamp <= ? " +
                        "AND s1.semantic_entity_id = s2.semantic_entity_id " +
                        "AND s1.location = ? AND s2.location = ? " +
                        "AND s1.timeStamp < s2.timeStamp " +
                        "AND s1.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp (2, new Timestamp(endTime.getTime()));
                    stmt.setTimestamp (3, new Timestamp(endTime.getTime()));
                    stmt.setString(4, startLocation);
                    stmt.setString(5, endLocation);

                    return externalQueryManager.explainQuery(stmt, 7);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery8(String userId, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                String query = "EXPLAIN EXTENDED SELECT u.name, s1.payload " +
                        "FROM SEMANTIC_OBSERVATION s1, SEMANTIC_OBSERVATION s2, SEMANTIC_OBSERVATION_TYPE st, USERS u " +
                        "WHERE s1.timeStamp >= ? AND s1.timeStamp <= ? " +
                        "AND s2.timeStamp = s1.timeStamp " +
                        "AND st.name = 'presence' AND s1.type_id = s2.type_id AND st.id = s1.type_id  " +
                        "AND s1.semantic_entity_id = ? AND s1.semantic_entity_id != s2.semantic_entity_id " +
                        "AND s1.payload['location'] = s2.payload['location'] AND s2.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp (2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, userId);

                    return externalQueryManager.explainQuery(stmt, 8);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }

            case 2:
                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();
                query = "EXPLAIN EXTENDED SELECT u.name, s1.location " +
                        "FROM PRESENCE s1, PRESENCE s2, USERS u " +
                        "WHERE s1.timeStamp >= ? AND s1.timeStamp <= ? " +
                        "AND s2.timeStamp = s1.timeStamp " +
                        "AND s1.semantic_entity_id = ? AND s1.semantic_entity_id != s2.semantic_entity_id " +
                        "AND s2.semantic_entity_id = u.id AND s1.location = s2.location ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp (2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, userId);

                    return externalQueryManager.explainQuery(stmt, 8);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery9(String userId, String infraTypeName) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "EXPLAIN EXTENDED SELECT Avg(timeSpent) as avgTimeSpent FROM " +
                        " (SELECT date_trunc('day', so.timeStamp), count(*)*10 as timeSpent " +
                        "  FROM SEMANTIC_OBSERVATION so, Infrastructure infra, Infrastructure_Type infraType, SEMANTIC_OBSERVATION_TYPE st " +
                        "  WHERE st.name = 'presence' AND so.type_id = st.id " +
                        "  AND so.payload['location'] = infra.id " +
                        "  AND infra.INFRASTRUCTURE_TYPE_ID = infraType.id AND infraType.name = ? " +
                        "  AND so.semantic_entity_id = ? " +
                        "  GROUP BY  date_trunc('day', so.timeStamp)) AS timeSpentPerDay";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString (1, infraTypeName);
                    stmt.setString(2, userId);

                    return externalQueryManager.explainQuery(stmt, 9);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                return externalQueryManager.explainQuery9(userId, infraTypeName);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery10(Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                return externalQueryManager.explainQuery10(startTime, endTime);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }
}
