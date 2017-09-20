package edu.uci.ics.tippers.data.postgresql;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.exception.BenchmarkException;

public class PgSQLDataUploader extends BaseDataUploader{

    public PgSQLDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
    }

    @Override
    public Database getDatabase() {
        return null;
    }

    @Override
    public void addAllData() throws BenchmarkException {

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
}
