package edu.uci.ics.tippers.query.postgresql;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.postgresql.PgSQLConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static edu.uci.ics.tippers.common.util.Helper.getFileFromQuery;

public class PgSQLQueryManager extends BaseQueryManager{

    private Connection connection;

    // Needed For External Databases
    private Database database = Database.POSTGRESQL;

    public PgSQLQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        connection = PgSQLConnectionManager.getInstance().getConnection();
    }

    // For External Database (CrateDB) Usage
    public PgSQLQueryManager( int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout,
                             Connection connection, Database database) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        this.connection = connection;
        this.database = database;
    }

    public Duration runTimedQuery(PreparedStatement stmt, int queryNum) throws BenchmarkException {
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

    public Duration explainQuery(PreparedStatement stmt, int queryNum) throws BenchmarkException {
        try {
            Instant start = Instant.now();
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            RowWriter<String> writer = new RowWriter<>(outputDir+"/explains/", getDatabase(), mapping, getFileFromQuery(queryNum));
            while(rs.next()) {
                StringBuilder line = new StringBuilder("");
                for(int i = 1; i <= columnsNumber; i++)
                    line.append(rs.getString(i)).append("\t");
                writer.writeString(line.toString());
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


    @Override
    public Database getDatabase() {
        return database;
    }

    @Override
    public void cleanUp() {

    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                String query = "SELECT name FROM SENSOR WHERE id=?";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString(1, sensorId);
                    return runTimedQuery(stmt, 1);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
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

                    Array locationsArray = connection.createArrayOf("VARCHAR", locationIds.toArray());
                    stmt.setArray(2, locationsArray);
                    return runTimedQuery(stmt, 2);
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
                String query = "SELECT timeStamp, payload FROM OBSERVATION WHERE timestamp>? AND timestamp<? " +
                        "AND SENSOR_ID=?";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, sensorId);

                    return runTimedQuery(stmt, 3);
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
                        query = "SELECT timeStamp, temperature FROM ThermometerObservation  WHERE timestamp>? AND timestamp<? " +
                            "AND SENSOR_ID=?";
                    else if ("WeMo".equals(typeId))
                        query = "SELECT timeStamp, currentMilliWatts, onTodaySeconds FROM WeMoObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=?";
                    else if ("WiFiAP".equals(typeId))
                        query = "SELECT timeStamp, clientId FROM WiFiAPObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=?";

                    stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, sensorId);

                    runTimedQuery(stmt, 3);

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
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "SELECT timeStamp, payload FROM OBSERVATION WHERE timestamp>? AND timestamp<? " +
                        "AND SENSOR_ID = ANY(?)";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                    Array sensorIdArray = connection.createArrayOf("VARCHAR", sensorIds.toArray());
                    stmt.setArray(3, sensorIdArray);

                    return runTimedQuery(stmt, 4);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT ID, SENSOR_TYPE_ID FROM SENSOR WHERE ID=ANY(?)";
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);
                    Array sensorIdArray = connection.createArrayOf("VARCHAR", sensorIds.toArray());
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

                        sensorIdArray = connection.createArrayOf("VARCHAR", thermoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        runTimedQuery(stmt, 4);
                    }
                    if (!wemoSensors.isEmpty()) {
                        query = "SELECT timeStamp, currentMilliWatts, onTodaySeconds FROM WeMoObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=ANY(?)";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("VARCHAR", wemoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        runTimedQuery(stmt, 4);
                    }
                    if (!wifiSensors.isEmpty()) {
                        query = "SELECT timeStamp, clientId FROM WiFiAPObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=ANY(?)";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("VARCHAR", wifiSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        runTimedQuery(stmt, 4);
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
                        "timestamp>? AND timestamp<? AND (payload::json ->> '%s')::int >= ? AND (payload::json ->> '%s')::int <= ? ",
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
                    return runTimedQuery(stmt, 5);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = String.format("SELECT * FROM %sOBSERVATION o " +
                        "WHERE timestamp>? AND timestamp<? AND %s>=? AND %s<=?", sensorTypeName,
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
                    return runTimedQuery(stmt, 5);
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
                        "(SELECT sensor_id, date_trunc('day', timestamp), " +
                        "count(*) as counts " +
                        "FROM OBSERVATION WHERE timestamp>? AND timestamp<? " +
                        "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                        "AS obs GROUP BY sensor_id";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                    Array sensorIdArray = connection.createArrayOf("VARCHAR", sensorIds.toArray());
                    stmt.setArray(3, sensorIdArray);

                    return runTimedQuery(stmt, 6);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT ID, SENSOR_TYPE_ID FROM SENSOR WHERE ID=ANY(?)";
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);
                    Array sensorIdArray = connection.createArrayOf("VARCHAR", sensorIds.toArray());
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

                        sensorIdArray = connection.createArrayOf("VARCHAR", thermoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        runTimedQuery(stmt, 6);
                    }
                    if (!wemoSensors.isEmpty()) {
                        query = "SELECT obs.sensor_id, avg(counts) FROM " +
                                "(SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM WeMoObservation WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("VARCHAR", wemoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        runTimedQuery(stmt, 6);
                    }
                    if (!wifiSensors.isEmpty()) {
                        query = "SELECT obs.sensor_id, avg(counts) FROM " +
                                "(SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM WiFiAPObservation WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("VARCHAR", wifiSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        runTimedQuery(stmt, 6);
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
        switch(mapping){
            case 1:
                String query = "SELECT u.name " +
                        "FROM SEMANTIC_OBSERVATION s1, SEMANTIC_OBSERVATION s2, SEMANTIC_OBSERVATION_TYPE st, USERS u " +
                        "WHERE date_trunc('day', s1.timeStamp) = ? AND date_trunc('day', s2.timeStamp) = ? " +
                        "AND st.name = 'presence' AND st.id = s1.type_id AND st.id = s2.type_id " +
                        "AND s1.semantic_entity_id = s2.semantic_entity_id " +
                        "AND s1.payload::json->>'location' = ? AND s2.payload::json->>'location' = ? " +
                        "AND s1.timeStamp < s2.timeStamp " +
                        "AND s1.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setDate (1, new java.sql.Date(date.getTime()));
                    stmt.setDate (2, new java.sql.Date(date.getTime()));
                    stmt.setString(3, startLocation);
                    stmt.setString(4, endLocation);
                    
                    return runTimedQuery(stmt, 7);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                 query = "SELECT u.name " +
                        "FROM PRESENCE s1, PRESENCE s2, USERS u " +
                        "WHERE date_trunc('day', s1.timeStamp) = ? AND date_trunc('day', s2.timeStamp) = ? " +
                        "AND s1.semantic_entity_id = s2.semantic_entity_id " +
                        "AND s1.location = ? AND s2.location = ? " +
                        "AND s1.timeStamp < s2.timeStamp " +
                        "AND s1.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setDate (1, new java.sql.Date(date.getTime()));
                    stmt.setDate (2, new java.sql.Date(date.getTime()));
                    stmt.setString(3, startLocation);
                    stmt.setString(4, endLocation);

                    return runTimedQuery(stmt, 7);
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
        switch(mapping){
            case 1:
                String query = "SELECT u.name, s1.payload " +
                        "FROM SEMANTIC_OBSERVATION s1, SEMANTIC_OBSERVATION s2, SEMANTIC_OBSERVATION_TYPE st, USERS u " +
                        "WHERE date_trunc('day', s1.timeStamp) = ? " +
                        "AND s2.timeStamp = s1.timeStamp " +
                        "AND st.name = 'presence' AND s1.type_id = s2.type_id AND st.id = s1.type_id  " +
                        "AND s1.semantic_entity_id = ? AND s1.semantic_entity_id != s2.semantic_entity_id " +
                        "AND s1.payload::json->>'location' = s2.payload::json->>'location' AND s2.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setDate (1, new java.sql.Date(date.getTime()));
                    stmt.setString(2, userId);
                    
                    return runTimedQuery(stmt, 8);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }

            case 2:
                query = "SELECT u.name, s1.location " +
                        "FROM PRESENCE s1, PRESENCE s2, USERS u " +
                        "WHERE date_trunc('day', s1.timeStamp) = ? " +
                        "AND s2.timeStamp = s1.timeStamp " +
                        "AND s1.semantic_entity_id = ? AND s1.semantic_entity_id != s2.semantic_entity_id " +
                        "AND s2.semantic_entity_id = u.id AND s1.location = s2.location ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setDate (1, new java.sql.Date(date.getTime()));
                    stmt.setString(2, userId);

                    return runTimedQuery(stmt, 8);
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
        switch(mapping){
            case 1:
                String query = "SELECT Avg(timeSpent) as avgTimeSpent FROM " +
                        " (SELECT date_trunc('day', so.timeStamp), count(*)*10 as timeSpent " +
                        "  FROM SEMANTIC_OBSERVATION so, Infrastructure infra, Infrastructure_Type infraType, SEMANTIC_OBSERVATION_TYPE st " +
                        "  WHERE st.name = 'presence' AND so.type_id = st.id " +
                        "  AND so.payload::json->>'location' = infra.id " +
                        "  AND infra.INFRASTRUCTURE_TYPE_ID = infraType.id AND infraType.name = ? " +
                        "  AND so.semantic_entity_id = ? " +
                        "  GROUP BY  date_trunc('day', so.timeStamp)) AS timeSpentPerDay";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString (1, infraTypeName);
                    stmt.setString(2, userId);

                    return runTimedQuery(stmt, 9);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT Avg(timeSpent) as avgTimeSpent FROM " +
                        " (SELECT date_trunc('day', so.timeStamp), count(*)*10 as timeSpent " +
                        "  FROM PRESENCE so, Infrastructure infra, Infrastructure_Type infraType " +
                        "  WHERE so.location = infra.id " +
                        "  AND infra.INFRASTRUCTURE_TYPE_ID = infraType.id AND infraType.name = ? " +
                        "  AND so.semantic_entity_id = ? " +
                        "  GROUP BY  date_trunc('day', so.timeStamp)) AS timeSpentPerDay";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString (1, infraTypeName);
                    stmt.setString(2, userId);

                    return runTimedQuery(stmt, 9);
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
        switch(mapping){
            case 1:
                String query = "SELECT infra.name, so.timeStamp, so.payload " +
                        "FROM SEMANTIC_OBSERVATION so, INFRASTRUCTURE infra, SEMANTIC_OBSERVATION_TYPE st " +
                        "WHERE so.timeStamp > ? AND so.timeStamp < ? " +
                        "AND so.type_id = 'occupancy' AND so.type_id = st.id AND so.semantic_entity_id = infra.id " +
                        "ORDER BY so.semantic_entity_id, so.timeStamp";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    return runTimedQuery(stmt, 10);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT infra.name, so.timeStamp, so.occupancy " +
                        "FROM OCCUPANCY so, INFRASTRUCTURE infra " +
                        "WHERE so.timeStamp > ? AND so.timeStamp < ? " +
                        "AND so.semantic_entity_id = infra.id " +
                        "ORDER BY so.semantic_entity_id, so.timeStamp";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    return runTimedQuery(stmt, 10);
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
                String query = "EXPLAIN SELECT name FROM SENSOR WHERE id=?";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString(1, sensorId);
                    return explainQuery(stmt, 1);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                String query = "EXPLAIN SELECT sen.name FROM SENSOR sen, SENSOR_TYPE st, " +
                        "COVERAGE_INFRASTRUCTURE ci WHERE sen.SENSOR_TYPE_ID=st.id AND st.name=? " +
                        "AND sen.id=ci.SENSOR_ID AND ci.INFRASTRUCTURE_ID=ANY(?)";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString(1, sensorTypeName);

                    Array locationsArray = connection.createArrayOf("VARCHAR", locationIds.toArray());
                    stmt.setArray(2, locationsArray);
                    return explainQuery(stmt, 2);
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
                String query = "EXPLAIN SELECT timeStamp, payload FROM OBSERVATION WHERE timestamp>? AND timestamp<? " +
                        "AND SENSOR_ID=?";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, sensorId);

                    return explainQuery(stmt, 3);
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
                        query = "EXPLAIN SELECT timeStamp, temperature FROM ThermometerObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=?";
                    else if ("WeMo".equals(typeId))
                        query = "EXPLAIN SELECT timeStamp, currentMilliWatts, onTodaySeconds FROM WeMoObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=?";
                    else if ("WiFiAP".equals(typeId))
                        query = "EXPLAIN SELECT timeStamp, clientId FROM WiFiAPObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=?";

                    stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    stmt.setString(3, sensorId);

                    explainQuery(stmt, 3);

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
    public Duration explainQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "EXPLAIN SELECT timeStamp, payload FROM OBSERVATION WHERE timestamp>? AND timestamp<? " +
                        "AND SENSOR_ID = ANY(?)";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                    Array sensorIdArray = connection.createArrayOf("VARCHAR", sensorIds.toArray());
                    stmt.setArray(3, sensorIdArray);

                    return explainQuery(stmt, 4);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT ID, SENSOR_TYPE_ID FROM SENSOR WHERE ID=ANY(?)";
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);
                    Array sensorIdArray = connection.createArrayOf("VARCHAR", sensorIds.toArray());
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
                        query = "EXPLAIN SELECT timeStamp, temperature FROM ThermometerObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=ANY(?)";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("VARCHAR", thermoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        explainQuery(stmt, 4);
                    }
                    if (!wemoSensors.isEmpty()) {
                        query = "EXPLAIN SELECT timeStamp, currentMilliWatts, onTodaySeconds FROM WeMoObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=ANY(?)";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("VARCHAR", wemoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        explainQuery(stmt, 4);
                    }
                    if (!wifiSensors.isEmpty()) {
                        query = "EXPLAIN SELECT timeStamp, clientId FROM WiFiAPObservation  WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID=ANY(?)";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("VARCHAR", wifiSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        explainQuery(stmt, 4);
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
                String query = String.format("EXPLAIN SELECT timeStamp, payload FROM OBSERVATION o, SENSOR s, SENSOR_TYPE st  " +
                                "WHERE s.id = o.sensor_id AND s.sensor_type_id=st.id AND st.name=? AND " +
                                "timestamp>? AND timestamp<? AND (payload::json ->> '%s')::int >= ? AND (payload::json ->> '%s')::int <= ? ",
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
                    return explainQuery(stmt, 5);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = String.format("EXPLAIN SELECT * FROM %sOBSERVATION o " +
                                "WHERE timestamp>? AND timestamp<? AND %s>=? AND %s<=?", sensorTypeName,
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
                    return explainQuery(stmt, 5);
                } catch (SQLException  e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                String query = "EXPLAIN SELECT obs.sensor_id, avg(counts) FROM " +
                        "( SELECT sensor_id, date_trunc('day', timestamp), " +
                        "count(*) as counts " +
                        "FROM OBSERVATION WHERE timestamp>? AND timestamp<? " +
                        "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                        "AS obs GROUP BY sensor_id";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                    Array sensorIdArray = connection.createArrayOf("VARCHAR", sensorIds.toArray());
                    stmt.setArray(3, sensorIdArray);

                    return explainQuery(stmt, 6);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "SELECT ID, SENSOR_TYPE_ID FROM SENSOR WHERE ID=ANY(?)";
                try {
                    Instant start = Instant.now();
                    PreparedStatement stmt = connection.prepareStatement(query);
                    Array sensorIdArray = connection.createArrayOf("VARCHAR", sensorIds.toArray());
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
                        query = "EXPLAIN SELECT obs.sensor_id, avg(counts) FROM " +
                                "( SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM ThermometerObservation WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("VARCHAR", thermoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        explainQuery(stmt, 6);
                    }
                    if (!wemoSensors.isEmpty()) {
                        query = "EXPLAIN SELECT obs.sensor_id, avg(counts) FROM " +
                                "( SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM WeMoObservation WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("VARCHAR", wemoSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        explainQuery(stmt, 6);
                    }
                    if (!wifiSensors.isEmpty()) {
                        query = "EXPLAIN SELECT obs.sensor_id, avg(counts) FROM " +
                                "( SELECT sensor_id, date_trunc('day', timestamp), " +
                                "count(*) as counts " +
                                "FROM WiFiAPObservation WHERE timestamp>? AND timestamp<? " +
                                "AND SENSOR_ID = ANY(?) GROUP BY sensor_id, date_trunc('day', timestamp)) " +
                                "AS obs GROUP BY sensor_id";
                        stmt = connection.prepareStatement(query);
                        stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
                        stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

                        sensorIdArray = connection.createArrayOf("VARCHAR", wifiSensors.toArray());
                        stmt.setArray(3, sensorIdArray);
                        explainQuery(stmt, 6);
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
        switch(mapping){
            case 1:
                String query = "EXPLAIN SELECT u.name " +
                        "FROM SEMANTIC_OBSERVATION s1, SEMANTIC_OBSERVATION s2, SEMANTIC_OBSERVATION_TYPE st, USERS u " +
                        "WHERE date_trunc('day', s1.timeStamp) = ? AND date_trunc('day', s2.timeStamp) = ? " +
                        "AND st.name = 'presence' AND st.id = s1.type_id AND st.id = s2.type_id " +
                        "AND s1.semantic_entity_id = s2.semantic_entity_id " +
                        "AND s1.payload::json->>'location' = ? AND s2.payload::json->>'location' = ? " +
                        "AND s1.timeStamp < s2.timeStamp " +
                        "AND s1.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setDate (1, new java.sql.Date(date.getTime()));
                    stmt.setDate (2, new java.sql.Date(date.getTime()));
                    stmt.setString(3, startLocation);
                    stmt.setString(4, endLocation);

                    return explainQuery(stmt, 7);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "EXPLAIN SELECT u.name " +
                        "FROM PRESENCE s1, PRESENCE s2, USERS u " +
                        "WHERE date_trunc('day', s1.timeStamp) = ? AND date_trunc('day', s2.timeStamp) = ? " +
                        "AND s1.semantic_entity_id = s2.semantic_entity_id " +
                        "AND s1.location = ? AND s2.location = ? " +
                        "AND s1.timeStamp < s2.timeStamp " +
                        "AND s1.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setDate (1, new java.sql.Date(date.getTime()));
                    stmt.setDate (2, new java.sql.Date(date.getTime()));
                    stmt.setString(3, startLocation);
                    stmt.setString(4, endLocation);

                    return explainQuery(stmt, 7);
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
        switch(mapping){
            case 1:
                String query = "EXPLAIN SELECT u.name, s1.payload " +
                        "FROM SEMANTIC_OBSERVATION s1, SEMANTIC_OBSERVATION s2, SEMANTIC_OBSERVATION_TYPE st, USERS u " +
                        "WHERE date_trunc('day', s1.timeStamp) = ? " +
                        "AND s2.timeStamp = s1.timeStamp " +
                        "AND st.name = 'presence' AND s1.type_id = s2.type_id AND st.id = s1.type_id  " +
                        "AND s1.semantic_entity_id = ? AND s1.semantic_entity_id != s2.semantic_entity_id " +
                        "AND s1.payload::json->>'location' = s2.payload::json->>'location' AND s2.semantic_entity_id = u.id ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setDate (1, new java.sql.Date(date.getTime()));
                    stmt.setString(2, userId);

                    return explainQuery(stmt, 8);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }

            case 2:
                query = "EXPLAIN SELECT u.name, s1.location " +
                        "FROM PRESENCE s1, PRESENCE s2, USERS u " +
                        "WHERE date_trunc('day', s1.timeStamp) = ? " +
                        "AND s2.timeStamp = s1.timeStamp " +
                        "AND s1.semantic_entity_id = ? AND s1.semantic_entity_id != s2.semantic_entity_id " +
                        "AND s2.semantic_entity_id = u.id AND s1.location = s2.location ";
                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setDate (1, new java.sql.Date(date.getTime()));
                    stmt.setString(2, userId);

                    return explainQuery(stmt, 8);
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
        switch(mapping){
            case 1:
                String query = "EXPLAIN SELECT Avg(timeSpent) as avgTimeSpent FROM " +
                        " ( SELECT date_trunc('day', so.timeStamp), count(*)*10 as timeSpent " +
                        "  FROM SEMANTIC_OBSERVATION so, Infrastructure infra, Infrastructure_Type infraType, SEMANTIC_OBSERVATION_TYPE st " +
                        "  WHERE st.name = 'presence' AND so.type_id = st.id " +
                        "  AND so.payload::json->>'location' = infra.id " +
                        "  AND infra.INFRASTRUCTURE_TYPE_ID = infraType.id AND infraType.name = ? " +
                        "  AND so.semantic_entity_id = ? " +
                        "  GROUP BY  date_trunc('day', so.timeStamp)) AS timeSpentPerDay";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString (1, infraTypeName);
                    stmt.setString(2, userId);

                    return explainQuery(stmt, 9);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "EXPLAIN SELECT Avg(timeSpent) as avgTimeSpent FROM " +
                        " ( SELECT date_trunc('day', so.timeStamp), count(*)*10 as timeSpent " +
                        "  FROM PRESENCE so, Infrastructure infra, Infrastructure_Type infraType " +
                        "  WHERE so.location = infra.id " +
                        "  AND infra.INFRASTRUCTURE_TYPE_ID = infraType.id AND infraType.name = ? " +
                        "  AND so.semantic_entity_id = ? " +
                        "  GROUP BY  date_trunc('day', so.timeStamp)) AS timeSpentPerDay";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setString (1, infraTypeName);
                    stmt.setString(2, userId);

                    return explainQuery(stmt, 9);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");

        }

    }

    @Override
    public Duration explainQuery10(Date startTime, Date endTime) throws BenchmarkException {
        switch(mapping){
            case 1:
                String query = "EXPLAIN SELECT infra.name, so.timeStamp, so.payload " +
                        "FROM SEMANTIC_OBSERVATION so, INFRASTRUCTURE infra, SEMANTIC_OBSERVATION_TYPE st " +
                        "WHERE so.timeStamp > ? AND so.timeStamp < ? " +
                        "AND so.type_id = 'occupancy' AND so.type_id = st.id AND so.semantic_entity_id = infra.id " +
                        "ORDER BY so.semantic_entity_id, so.timeStamp";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    return explainQuery(stmt, 10);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            case 2:
                query = "EXPLAIN SELECT infra.name, so.timeStamp, so.occupancy " +
                        "FROM OCCUPANCY so, INFRASTRUCTURE infra " +
                        "WHERE so.timeStamp > ? AND so.timeStamp < ? " +
                        "AND so.semantic_entity_id = infra.id " +
                        "ORDER BY so.semantic_entity_id, so.timeStamp";

                try {
                    PreparedStatement stmt = connection.prepareStatement(query);
                    stmt.setTimestamp (1, new Timestamp(startTime.getTime()));
                    stmt.setTimestamp(2, new Timestamp(endTime.getTime()));
                    return explainQuery(stmt, 10);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Query");
                }
            default:
                throw new BenchmarkException("No Such Mapping");


        }
    }


}
