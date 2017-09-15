package edu.uci.ics.tippers.data.griddb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.data.BaseDataUploader;

public class GridDBDataUploader extends BaseDataUploader {

    public GridDBDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
    }

    @Override
    public Database getDatabase() {
        return Database.GRIDDB;
    }

    @Override
    public void addAllData() {

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
