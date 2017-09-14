package edu.uci.ics.tippers.schema.griddb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.schema.BaseSchema;

public class GridDBSchema extends BaseSchema {

    public GridDBSchema(int mapping) {
        super(mapping);
    }

    @Override
    public Database getDatabase() {
        return null;
    }

    @Override
    public void createSchema() {

    }

    @Override
    public void dropSchema() {

    }

}
