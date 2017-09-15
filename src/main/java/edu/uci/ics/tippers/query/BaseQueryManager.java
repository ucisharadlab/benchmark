package edu.uci.ics.tippers.query;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class BaseQueryManager {

    protected int mapping;
    protected boolean writeOutput;

    public BaseQueryManager(int mapping, boolean writeOutput) {
        this.mapping = mapping;
        this.writeOutput = writeOutput;
    }

    public abstract Database getDatabase();

    public int getMapping() {
        return this.mapping;
    }

    public int setMapping() {
        return this.mapping;
    }

    public abstract Map<Integer, Duration> runQueries() throws BenchmarkException;

    public abstract Duration runQuery1(String sensorId) throws BenchmarkException;

    public abstract Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException;

    public abstract Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException;

    public abstract Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException;

    public abstract Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                                   Object startPayloadValue, Object endPayloadValue) throws BenchmarkException;

}
