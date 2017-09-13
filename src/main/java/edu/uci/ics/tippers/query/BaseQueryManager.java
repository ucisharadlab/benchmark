package edu.uci.ics.tippers.query;

import java.util.Date;
import java.util.List;

public abstract class BaseQueryManager {

    protected int mapping;

    public BaseQueryManager(int mapping) {
        this.mapping = mapping;
    }

    public int getMapping() {
        return this.mapping;
    }

    public int setMapping() {
        return this.mapping;
    }

    public abstract void runQuery1(String sensorId);

    public abstract void runQuery2(String sensorTypeName, List<String> locationIds);

    public abstract void runQuery3(String sensorId, Date startTime, Date endTime);

    public abstract void runQuery4(List<String> sensorIds, Date startTime, Date endTime);

    public abstract void runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                                   Object startPayloadValue, Object endPayloadValue);

}
