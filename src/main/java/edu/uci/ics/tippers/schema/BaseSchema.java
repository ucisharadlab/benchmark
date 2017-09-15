package edu.uci.ics.tippers.schema;

import edu.uci.ics.tippers.common.Database;

public abstract class BaseSchema {

    protected int mapping;

    public BaseSchema(int mapping) {
        this.mapping = mapping;
    }

    public int getMapping() {
        return mapping;
    }

    public void setMapping(int mapping) {
        this.mapping = mapping;
    }

    public abstract Database getDatabase();

    public abstract void createSchema();

    public abstract void dropSchema();


}
