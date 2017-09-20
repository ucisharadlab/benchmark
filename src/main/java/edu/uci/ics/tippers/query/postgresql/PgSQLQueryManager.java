package edu.uci.ics.tippers.query.postgresql;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;

import java.time.Duration;
import java.util.Date;
import java.util.List;

public class PgSQLQueryManager extends BaseQueryManager{

    public PgSQLQueryManager(int mapping, String queriesDir, boolean writeOutput) {
        super(mapping, queriesDir, writeOutput);
    }

    @Override
    public Database getDatabase() {
        return Database.POSTGRESQL;
    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        return Constants.MAX_DURATION;
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
}
