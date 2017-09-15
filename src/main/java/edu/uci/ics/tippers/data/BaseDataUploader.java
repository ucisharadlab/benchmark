package edu.uci.ics.tippers.data;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;

public abstract class BaseDataUploader {

    protected int mapping;
    protected String dataDir;

    public BaseDataUploader(int mapping, String dataDir) {
        this.mapping = mapping;
        this.dataDir = dataDir;
    }

    public int getMapping() {
        return mapping;
    }

    public void setMapping(int mapping) {
        this.mapping = mapping;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public abstract Database getDatabase();

    public abstract void addAllData() throws BenchmarkException;

    public abstract void addInfrastructureData() throws BenchmarkException;

    public abstract void addUserData() throws BenchmarkException;

    public abstract void addSensorData() throws BenchmarkException;

    public abstract void addDeviceData() throws BenchmarkException;

    public abstract void addObservationData() throws BenchmarkException;

    public abstract void virtualSensorData();

    public abstract void addSemanticObservationData();
}
