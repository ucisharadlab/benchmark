package edu.uci.ics.tippers.schema.cratedb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;

public class CrateDBSchema extends BaseSchema{

    public CrateDBSchema(int mapping, String dataDir) {
        super(mapping, dataDir);
    }

    @Override
    public Database getDatabase() {
        return Database.CRATEDB;
    }

    @Override
    public void createSchema() throws BenchmarkException {

    }

    @Override
    public void dropSchema() throws BenchmarkException {

    }
}
