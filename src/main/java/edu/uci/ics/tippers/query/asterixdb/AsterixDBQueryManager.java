package edu.uci.ics.tippers.query.asterixdb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.Helper;
import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class AsterixDBQueryManager extends BaseQueryManager{

    private AsterixDBConnectionManager connectionManager;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public AsterixDBQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout){
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        connectionManager = AsterixDBConnectionManager.getInstance();
    }

    @Override
    public Database getDatabase() {
        return Database.ASTERIXDB;
    }

    @Override
    public void cleanUp() {

    }

    private Duration runTimedQuery (String query, int queryNum) throws BenchmarkException {
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

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                    String.format("SELECT name FROM Sensor WHERE id = \"%s\";", sensorId), 1
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
                                sensorTypeName), 2
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
                                sensorId, sdf.format(startTime), sdf.format(endTime)), 3
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
                                sdf.format(startTime), sdf.format(endTime)),  4
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
                return runTimedQuery(
                        String.format("SELECT timeStamp, sensor.id, payload " +
                            "FROM Observation " +
                            "WHERE sensor.sensorType.name = \"%s\" AND timeStamp >= datetime(\"%s\") AND " +
                            "timeStamp <= datetime(\"%s\") " +
                            "AND payload.%s >= %s AND payload.%s <= %s",
                                sensorTypeName, sdf.format(startTime), sdf.format(endTime), payloadAttribute,
                                startPayloadValue, payloadAttribute, endPayloadValue), 5
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }


    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT obs.id , AVG(obs.count) FROM " +
                                "(SELECT sensor.id , get_date_from_datetime(timeStamp), count(*)  AS count " +
                                "FROM Observation " +
                                "WHERE sensor.id IN {{ " +
                                sensorIds.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(",")) +
                                " }} AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\") " +
                                "GROUP BY sensor.id, get_date_from_datetime(timeStamp)) AS obs GROUP BY obs.id",
                                sdf.format(startTime), sdf.format(endTime)), 6
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }
}
