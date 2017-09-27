package edu.uci.ics.tippers.data.griddb;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.griddb.StoreManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.data.griddb.mappings.GridDBDataMapping1;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.time.Duration;
import java.time.Instant;

public class GridDBDataUploader extends BaseDataUploader {

    private GridStore gridStore;
    private GridDBBaseDataMapping dataMapping;

    public GridDBDataUploader(int mapping, String dataDir) throws BenchmarkException {
        super(mapping, dataDir);
        gridStore = StoreManager.getInstance().getGridStore();
        switch (mapping) {
            case 1:
                dataMapping = new GridDBDataMapping1(gridStore, dataDir);
                break;
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Database getDatabase() {
        return Database.GRIDDB;
    }

    @Override
    public Duration addAllData() throws BenchmarkException {
        try {
            Instant start = Instant.now();
            dataMapping.addAll();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (GSException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Inserting Data");
        }
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
