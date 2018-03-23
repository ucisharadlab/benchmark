package edu.uci.ics.tippers.data.jana;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.time.Duration;

public class JanaDataUploader extends BaseDataUploader {


    public JanaDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
    }

    @Override
    public Database getDatabase() {
        return null;
    }

    @Override
    public Duration addAllData() throws BenchmarkException {
        return null;
    }

    @Override
    public void addInfrastructureData() throws BenchmarkException {

    }

    @Override
    public void addUserData() throws BenchmarkException {

    }

    @Override
    public void addSensorData() throws BenchmarkException {

    }

    @Override
    public void addDeviceData() throws BenchmarkException {

    }

    @Override
    public void addObservationData() throws BenchmarkException {

    }

    @Override
    public void virtualSensorData() {

    }

    @Override
    public void addSemanticObservationData() {

    }

    @Override
    public Duration insertPerformance() throws BenchmarkException {
        return null;
    }
}
