package edu.uci.ics.tippers.query.influxdb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;
import org.codehaus.jackson.map.Serializers;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import static edu.uci.ics.tippers.common.util.Helper.getFileFromQuery;

public class InfluxDBQueryManager extends BaseQueryManager {

    private Connection metadataConnection;


    public InfluxDBQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
    }

    @Override
    public Database getDatabase() {
        return Database.INFLUXDB;
    }

    @Override
    public void cleanUp() {

    }

    public Duration runTimedMetadataQuery(PreparedStatement stmt, int queryNum) throws BenchmarkException {
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
        return null;
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        return null;
    }

    @Override
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute, Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        return null;
    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        return null;
    }

    @Override
    public Duration runQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        return null;
    }

    @Override
    public Duration runQuery8(String userId, Date date) throws BenchmarkException {
        return null;
    }

    @Override
    public Duration runQuery9(String userId, String infraTypeName) throws BenchmarkException {
        return null;
    }

    @Override
    public Duration runQuery10(Date startTime, Date endTime) throws BenchmarkException {
        return null;
    }
}
