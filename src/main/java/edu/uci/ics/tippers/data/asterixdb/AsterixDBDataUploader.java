package edu.uci.ics.tippers.data.asterixdb;

import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;

public class AsterixDBDataUploader extends BaseDataUploader {

    private AsterixDBConnectionManager connectionManager;

    public AsterixDBDataUploader(int mapping) {
        super(mapping);
        connectionManager = AsterixDBConnectionManager.getInstance();
    }

    @Override
    public void addInfrastructureData() {

    }

    @Override
    public void addUserData() {

    }

    @Override
    public void addSensorData() {

    }

    @Override
    public void addDeviceData() {

    }

    @Override
    public void addObservationData() {

    }

    @Override
    public void virtualSensorData() {

    }

    @Override
    public void addSemanticObservationData() {

    }
}
