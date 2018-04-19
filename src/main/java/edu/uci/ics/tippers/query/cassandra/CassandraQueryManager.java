package edu.uci.ics.tippers.query.cassandra;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;

import java.time.Duration;
import java.util.Date;
import java.util.List;

/**
 * Created by peeyush on 21/9/17.
 */
public class CassandraQueryManager extends BaseQueryManager {

    public CassandraQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
    }

    @Override
    public Database getDatabase() {
        return null;
    }

    @Override
    public void cleanUp() {

    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        return null;
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        return null;
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

    @Override
    public Duration runQuery11() throws BenchmarkException {
        return null;
    }
}
