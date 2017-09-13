package edu.uci.ics.tippers.query.asterixdb;

import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import edu.uci.ics.tippers.query.BaseQueryManager;

import java.util.Date;
import java.util.List;

public class AsterixDBQueryManager extends BaseQueryManager{

    private AsterixDBConnectionManager connectionManager;

    public AsterixDBQueryManager(int mapping){
        super(mapping);
        connectionManager = AsterixDBConnectionManager.getInstance();
    }

    @Override
    public void runQuery1(String sensorId) {
        connectionManager.sendQuery("");
    }

    @Override
    public void runQuery2(String sensorTypeName, List<String> locationIds) {
        connectionManager.sendQuery("");
    }

    @Override
    public void runQuery3(String sensorId, Date startTime, Date endTime) {
        connectionManager.sendQuery("");
    }

    @Override
    public void runQuery4(List<String> sensorIds, Date startTime, Date endTime) {
        connectionManager.sendQuery("");
    }

    @Override
    public void runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                          Object startPayloadValue, Object endPayloadValue) {

    }

}
