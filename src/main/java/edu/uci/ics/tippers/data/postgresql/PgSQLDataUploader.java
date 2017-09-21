package edu.uci.ics.tippers.data.postgresql;

import com.toshiba.mwcloud.gs.GridStore;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.postgresql.PgSQLConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.data.postgresql.mappings.DataMapping1;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.sql.Connection;

public class PgSQLDataUploader extends BaseDataUploader{

    private Connection connection;
    private DataMapping1 dataMapping;

    public PgSQLDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
        connection = PgSQLConnectionManager.getInstance().getConnection();
        switch (mapping) {
            case 1:
                dataMapping = new DataMapping1(connection, dataDir);
                break;
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Database getDatabase() {
        return Database.POSTGRESQL;
    }

    @Override
    public void addAllData() throws BenchmarkException {
        dataMapping.addAll();
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
