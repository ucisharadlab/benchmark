package edu.uci.ics.tippers.data.sqlserver;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.sqlserver.SQLServerConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.data.sqlserver.mappings.SQLServerDataMapping1;
import edu.uci.ics.tippers.data.sqlserver.mappings.SQLServerDataMapping2;
import edu.uci.ics.tippers.data.sqlserver.mappings.SQLServerDataMapping3;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;

public class SQLServerDataUploader extends BaseDataUploader {
    private Connection connection;
    private SQLServerBaseDataMapping dataMapping;

    public SQLServerDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
        connection = SQLServerConnectionManager.getInstance().getConnection();
        switch (mapping) {
            case 1:
                dataMapping = new SQLServerDataMapping1(connection, dataDir);
                break;
            case 2:
                dataMapping = new SQLServerDataMapping2(connection, dataDir);
                break;
            case 3:
                connection = SQLServerConnectionManager.getInstance().getEncryptedConnection();
                dataMapping = new SQLServerDataMapping3(connection, dataDir);
                break;
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Database getDatabase() {
        return Database.SQLSERVER;
    }

    @Override
    public Duration addAllData() throws BenchmarkException {
        Instant start = Instant.now();
        dataMapping.addAll();
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
        dataMapping.insertPerformance();
        Instant end = Instant.now();
        return Duration.between(start, end);

    }
}
