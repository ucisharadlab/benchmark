package edu.uci.ics.tippers.schema.jana;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;

public class JanaSchema extends BaseSchema {

    public JanaSchema(int mapping, String dataDir) {
        super(mapping, dataDir);
    }

    @Override
    public Database getDatabase() {
        return null;
    }

    @Override
    public void createSchema() throws BenchmarkException {

    }

    @Override
    public void dropSchema() throws BenchmarkException {

    }
}
