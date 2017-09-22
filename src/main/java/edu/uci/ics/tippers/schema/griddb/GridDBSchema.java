package edu.uci.ics.tippers.schema.griddb;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.griddb.StoreManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;
import edu.uci.ics.tippers.schema.griddb.mappings.GridDBSchemaMapping1;

public class GridDBSchema extends BaseSchema {

    private GridDBBaseSchemaMapping schemaMapping;
    private GridStore gridStore;

    public GridDBSchema(int mapping, String dataDir) throws BenchmarkException {
        super(mapping, dataDir);
        gridStore = StoreManager.getInstance().getGridStore();
        switch (mapping) {
            case 1:
                schemaMapping = new GridDBSchemaMapping1(gridStore, dataDir);
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
    public void createSchema() throws BenchmarkException {
        try {
            schemaMapping.createAll();
        } catch (GSException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Creating Schema");
        }
    }

    @Override
    public void dropSchema() throws BenchmarkException {
        try {
            schemaMapping.deleteAll();
        } catch (GSException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error deleting Schema");
        }
    }

}
