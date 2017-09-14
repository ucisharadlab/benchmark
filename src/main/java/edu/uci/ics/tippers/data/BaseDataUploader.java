package edu.uci.ics.tippers.data;

import edu.uci.ics.tippers.common.Database;

public abstract class BaseDataUploader {

    protected int mapping;

    public BaseDataUploader(int mapping) {
        this.mapping = mapping;
    }

    public abstract Database getDatabase();

    public abstract void addAllData();

    public abstract void addInfrastructureData();

    public abstract void addUserData();

    public abstract void addSensorData();

    public abstract void addDeviceData();

    public abstract void addObservationData();

    public abstract void virtualSensorData();

    public abstract void addSemanticObservationData();
}
