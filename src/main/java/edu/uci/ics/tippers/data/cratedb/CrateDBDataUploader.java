package edu.uci.ics.tippers.data.cratedb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.cratedb.CrateDBConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.data.postgresql.PgSQLBaseDataMapping;
import edu.uci.ics.tippers.data.postgresql.mappings.PgSQLDataMapping1;
import edu.uci.ics.tippers.data.postgresql.mappings.PgSQLDataMapping2;
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
        switch (mapping) {
            case 1:
                externalDataMapping = new PgSQLDataMapping1(connection, dataDir);
                break;
            case 2:
                externalDataMapping = new PgSQLDataMapping2(connection, dataDir);
                break;
            default:
                throw new BenchmarkException("No Such Mapping");
        }
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
            case 2:
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

    @Override
    public Duration insertPerformance() throws BenchmarkException {
        Instant start = Instant.now();
        externalDataMapping.insertPerformance();
        Instant end = Instant.now();
        return Duration.between(start, end);
    }
}
