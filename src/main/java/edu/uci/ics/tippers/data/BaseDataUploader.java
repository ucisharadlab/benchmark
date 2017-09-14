package edu.uci.ics.tippers.data;

public abstract class BaseDataUploader {

    protected int mapping;

    public BaseDataUploader(int mapping) {
        this.mapping = mapping;
    }

    public abstract void addInfrastructureData();

    public abstract void addUserData();

    public abstract void addSensorData();

    public abstract void addDeviceData();

    public abstract void addObservationData();

    public abstract void virtualSensorData();

    public abstract void addSemanticObservationData();
}
