package edu.uci.ics.tippers.data.cratedb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.cratedb.CrateDBConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.data.postgresql.PgSQLBaseDataMapping;
import edu.uci.ics.tippers.data.postgresql.mappings.PgSQLDataMapping1;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;

public class CrateDBDataUploader extends BaseDataUploader{

    private Connection connection;
    private PgSQLBaseDataMapping externalDataMapping;

    public CrateDBDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
        connection = CrateDBConnectionManager.getInstance().getConnection();
        externalDataMapping = new PgSQLDataMapping1(connection, dataDir);
    }

    @Override
    public Database getDatabase() {
        return Database.CRATEDB;
    }

    @Override
    public Duration addAllData() throws BenchmarkException {
        Instant start = Instant.now();
        switch (mapping){
            case 1:
                externalDataMapping.addAll();
                break;
            default:
                throw new BenchmarkException("Error Uploading Data");
        }
        Instant end = Instant.now();
        return Duration.between(start, end);
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
