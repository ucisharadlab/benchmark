package edu.uci.ics.tippers.query.asterixdb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.query.QueryCSVReader;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class AsterixDBQueryManager extends BaseQueryManager{

    private AsterixDBConnectionManager connectionManager;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public AsterixDBQueryManager(int mapping, String queriesDir, boolean writeOutput){
        super(mapping, queriesDir, writeOutput);
        connectionManager = AsterixDBConnectionManager.getInstance();
    }

    @Override
    public Database getDatabase() {
        return Database.ASTERIXDB;
    }

    private Duration runTimedQuery (String query) throws BenchmarkException {
        Instant startTime = Instant.now();
        HttpResponse response = connectionManager.sendQuery(query);
        Instant endTime = Instant.now();

        if (writeOutput) {
            // TODO: Write To File
            try {
                System.out.println(EntityUtils.toString(response.getEntity()));
            } catch (IOException e) {
                e.printStackTrace();
                throw new BenchmarkException("Error writing output to file");
            }
        }
        return Duration.between(startTime, endTime);
    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                    String.format("SELECT name FROM Sensor WHERE id = \"%s\";", sensorId)
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {

        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT s.id, s.name FROM Sensor s WHERE s.sensorType.name=\"%s\" AND "
                                        + "(SOME e IN s.coverage.entitiesCovered SATISFIES e.id IN {{"
                                        + locationIds.stream().map(e -> "\"" + e + "\"" ).collect(Collectors.joining(","))
                                        + "}});",
                                sensorTypeName)
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT timeStamp, sensor.id, payload FROM Observation WHERE sensor.id=\"%s\" "
                                + "AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\");",
                                sensorId, sdf.format(startTime), sdf.format(endTime))
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT timeStamp, sensor.id, payload FROM Observation WHERE sensor.id IN {{ "
                                        + sensorIds.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(","))
                                        + " }} AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\");",
                                sdf.format(startTime), sdf.format(endTime))
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                          Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        switch (mapping) {
            case 1:
                // TODO: Implement Query 5
                throw new BenchmarkException("Not Implemented Yet");
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

}
