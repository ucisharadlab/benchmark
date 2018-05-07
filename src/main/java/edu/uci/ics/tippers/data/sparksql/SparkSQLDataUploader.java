package edu.uci.ics.tippers.data.sparksql;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.sparksql.SparkSQLConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.data.postgresql.PgSQLBaseDataMapping;
import edu.uci.ics.tippers.data.postgresql.mappings.PgSQLDataMapping1;
import edu.uci.ics.tippers.data.postgresql.mappings.PgSQLDataMapping2;
import edu.uci.ics.tippers.data.sparksql.mappings.SparkSQLDataMapping1;
import edu.uci.ics.tippers.data.sparksql.mappings.SparkSQLDataMapping2;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;

public class SparkSQLDataUploader extends BaseDataUploader{

    private Connection connection;
    private PgSQLBaseDataMapping externalDataMapping;

    public SparkSQLDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
        connection = SparkSQLConnectionManager.getInstance().getConnection();
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
                new SparkSQLDataMapping1(connection, dataDir).addAll();
                break;
            case 2:
                new SparkSQLDataMapping2(connection, dataDir).addAll();
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
        switch (mapping) {
	        case 1:
		        new SparkSQLDataMapping1(connection, dataDir).insertPerformance();
		        break;
            case 2:
                new SparkSQLDataMapping2(connection, dataDir).insertPerformance();
                break;
	}
        Instant end = Instant.now();
        return Duration.between(start, end);
    }
}
