package edu.uci.ics.tippers.data.griddb;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.data.griddb.mappings.DataMapping1;
import edu.uci.ics.tippers.exception.BenchmarkException;

public class GridDBDataUploader extends BaseDataUploader {

    private GridStore gridStore;
    private BaseDataMapping dataMapping;

    public GridDBDataUploader(int mapping, String dataDir) throws BenchmarkException {
        super(mapping, dataDir);
        switch (mapping) {
            case 1:
                dataMapping = new DataMapping1(gridStore, dataDir);
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
    public void addAllData() throws BenchmarkException {
        try {
            dataMapping.addAll();
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
