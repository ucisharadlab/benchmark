package edu.uci.ics.tippers.schema.mongodb;

import com.mongodb.client.MongoDatabase;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.mongodb.DBManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;

/**
 * Created by peeyush on 19/9/17.
 */
public class MongoDBSchema extends BaseSchema {

    private MongoDatabase database;

    public MongoDBSchema(int mapping, String dataDir) {
        super(mapping, dataDir);
        database = DBManager.getInstance().getDatabase();
    }

    @Override
    public Database getDatabase() {
        return Database.MONGODB;
    }

    @Override
    public void createSchema() throws BenchmarkException {
        database.createCollection("Location");
        database.createCollection("InfrastructureType");
        database.createCollection("Infrastructure");
        database.createCollection("Group");
        database.createCollection("User");
        database.createCollection("PlatformType");
        database.createCollection("Platform");

        database.createCollection("SensorType");
        database.createCollection("Sensor");
        database.createCollection("Observation");

        database.createCollection("SemanticObservationType");
        database.createCollection("VirtualSensorType");
        database.createCollection("VirtualSensor");
        database.createCollection("SemanticObservation");
    }

    @Override
    public void dropSchema() throws BenchmarkException {
        DBManager.getInstance().dropDatabase();
    }
}
