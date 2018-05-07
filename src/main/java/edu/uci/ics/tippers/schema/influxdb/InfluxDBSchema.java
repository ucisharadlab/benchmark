package edu.uci.ics.tippers.schema.influxdb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.influxdb.InfluxDBConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;
import org.apache.http.HttpResponse;

public class InfluxDBSchema extends BaseSchema{

    public InfluxDBSchema(int mapping, String dataDir) {
        super(mapping, dataDir);
    }

    @Override
    public Database getDatabase() {
        return Database.INFLUXDB;
    }

    @Override
    public void createSchema() throws BenchmarkException {
        HttpResponse response = InfluxDBConnectionManager.getInstance().addSchema();
    }

    @Override
    public void dropSchema() throws BenchmarkException {
        HttpResponse response = InfluxDBConnectionManager.getInstance().deleteSchema();
    }
}
