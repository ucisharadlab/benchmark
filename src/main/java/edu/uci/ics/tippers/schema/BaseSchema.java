package edu.uci.ics.tippers.schema;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;

public abstract class BaseSchema {

    protected int mapping;
    protected String dataDir;

    public BaseSchema(int mapping, String dataDir) {
        this.mapping = mapping;
        this.dataDir = dataDir;
    }

    public int getMapping() {
        return mapping;
    }

    public void setMapping(int mapping) {
        this.mapping = mapping;
    }

    public abstract Database getDatabase();

    public abstract void createSchema() throws BenchmarkException;

    public abstract void dropSchema() throws BenchmarkException;


}
