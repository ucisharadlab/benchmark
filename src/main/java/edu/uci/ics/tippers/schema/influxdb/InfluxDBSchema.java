package edu.uci.ics.tippers.schema.influxdb;

import com.ibatis.common.jdbc.ScriptRunner;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.influxdb.InfluxDBConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;
import org.apache.http.HttpResponse;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;

public class InfluxDBSchema extends BaseSchema{

    private Connection metadataConnection;
    private String CREATE_FORMAT = "influxdb/influxdb_metadata_create.sql";
    private String DROP_FORMAT = "influxdb/influxdb_metadata_drop.sql";

    public InfluxDBSchema(int mapping, String dataDir) {
        super(mapping, dataDir);
    }

    private void runScript(String fileName) throws BenchmarkException {
        ScriptRunner sr = new ScriptRunner(metadataConnection, false, true);
        sr.setLogWriter(null);
        Reader reader;
        try {
            InputStream inputStream = InfluxDBSchema.class.getClassLoader().getResourceAsStream(fileName);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            sr.runScript(reader);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running SQL script");
        }
    }

    @Override
    public Database getDatabase() {
        return Database.INFLUXDB;
    }

    @Override
    public void createSchema() throws BenchmarkException {
        HttpResponse response = InfluxDBConnectionManager.getInstance().addSchema();
        //runScript(CREATE_FORMAT);
    }

    @Override
    public void dropSchema() throws BenchmarkException {
        HttpResponse response = InfluxDBConnectionManager.getInstance().deleteSchema();
        //runScript(DROP_FORMAT);
    }
}
