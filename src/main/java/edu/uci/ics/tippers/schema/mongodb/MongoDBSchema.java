package edu.uci.ics.tippers.schema.mongodb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;

/**
 * Created by peeyush on 19/9/17.
 */
public class MongoDBSchema extends BaseSchema {

    public MongoDBSchema(int mapping, String dataDir) {
        super(mapping, dataDir);
    }

    @Override
    public Database getDatabase() {
        return Database.MONGODB;
    }

    @Override
    public void createSchema() throws BenchmarkException {

    }

    @Override
    public void dropSchema() throws BenchmarkException {

    }
}
