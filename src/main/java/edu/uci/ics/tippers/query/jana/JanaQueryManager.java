package edu.uci.ics.tippers.query.jana;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.Helper;
import edu.uci.ics.tippers.connection.jana.JanaConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.query.asterixdb.AsterixDBQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;

public class JanaQueryManager extends BaseQueryManager {

    private static final Logger LOGGER = Logger.getLogger(AsterixDBQueryManager.class);
    private JanaConnectionManager connectionManager;

    public JanaQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        connectionManager = JanaConnectionManager.getInstance();
    }

    @Override
    public Database getDatabase() {
        return null;
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

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                return runTimedQuery(
                        String.format("SELECT name FROM SENSOR WHERE id = \"%s\";", sensorId), 1
                );
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
        return Constants.MAX_DURATION;
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
}
