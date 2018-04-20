package edu.uci.ics.tippers.query.pulsar;

import edu.uci.ics.tippers.common.DataType;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.Helper;
import edu.uci.ics.tippers.connection.pulsar.PulsarConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.operators.AggregateOperator;
import edu.uci.ics.tippers.operators.GroupBy;
import edu.uci.ics.tippers.operators.Join;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.query.asterixdb.AsterixDBQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import java.io.IOException;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class PulsarQueryManager extends BaseQueryManager {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final Logger LOGGER = Logger.getLogger(PulsarQueryManager.class);
    private PulsarConnectionManager connectionManager;

    public PulsarQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        connectionManager = PulsarConnectionManager.getInstance();
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

    private JSONArray runQueryWithResults(String query) throws BenchmarkException{
        HttpResponse response = connectionManager.sendQuery(query);
        try {
            return new JSONArray(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error writing output to file");
        }
    }

    @Override
    public Database getDatabase() {
        return Database.PULSAR;
    }

    @Override
    public void cleanUp() {

    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
//                return runTimedQuery(
//                        String.format("SELECT SENSOR_NAME FROM TippersBigTable WHERE ROW_TYPE='SENSOR' " +
//                                "AND SENSOR_ID='%s';", sensorId), 1
//                );
                return Constants.MAX_DURATION;

            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
//                return runTimedQuery(
//                        String.format("SELECT OBSERVATION_TIMESTAMP, OBSERVATION_clientId FROM TippersBigTable " +
//                                "WHERE OBSERVATION_TIMESTAMP>'%s' AND OBSERVATION_TIMESTAMP<'%s' AND ROW_TYPE='%s' " +
//                                "AND OBSERVATION_SENSOR_ID='%s'", sdf.format(startTime), sdf.format(endTime),
//                                "WiFiAPObservation", sensorId), 3
//                );
                return Constants.MAX_DURATION;

            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute, Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        return Constants.MAX_DURATION;
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

    @Override
    public Duration runQuery11() throws BenchmarkException {
        Instant startTime = Instant.now();
        JSONArray platforms = runQueryWithResults("SELECT PLATFORM_USER_ID, PLATFORM_HASHED_MAC FROM TippersBigTable " +
                "WHERE ROW_TYPE='PLATFORM'");
        String rightTableQuery = "SELECT OBSERVATION_ID FROM TippersBigTable WHERE ROW_TYPE='WiFiAPObservation' " +
                "AND OBSERVATION_clientId='%s'";
        JSONArray joinResults = AggregateOperator.count(GroupBy.doGroupByIndex(
                Join.indexLoopJoin(platforms, rightTableQuery, 1, DataType.STRING, connectionManager),
                Arrays.asList(1)), Arrays.asList(1));

        if (writeOutput) {
            try {
                RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping,
                        Helper.getFileFromQuery(11));
                writer.writeString(joinResults.toString());
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new BenchmarkException("Error writing output to file");
            }
        }
        Instant endTime = Instant.now();
        return Duration.between(startTime, endTime);
    }
}
