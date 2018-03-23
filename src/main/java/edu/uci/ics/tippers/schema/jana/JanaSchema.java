package edu.uci.ics.tippers.schema.jana;

import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.jana.JanaConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;
import edu.uci.ics.tippers.schema.asterixdb.AsterixDBSchema;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class JanaSchema extends BaseSchema {

    private JanaConnectionManager connectionManager;
    private JSONParser parser = new JSONParser();
    private String CREATE_SCHEMA_FILE = "jana/schema/create.json";
    private String DROP_SCHEMA_FILE = "jana/schema/drop.json";


    public JanaSchema(int mapping, String dataDir) {
        super(mapping, dataDir);
        connectionManager = JanaConnectionManager.getInstance();
    }

    @Override
    public Database getDatabase() {
        return Database.JANA;
    }

    @Override
    public void createSchema() throws BenchmarkException {
        try {
            JSONArray schemaJson = (JSONArray) parser.parse(new InputStreamReader(
                    JanaSchema.class.getClassLoader().getResourceAsStream(CREATE_SCHEMA_FILE)));
            connectionManager.createRelations(schemaJson);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error While Creating Schema");
        }

    }

    @Override
    public void dropSchema() throws BenchmarkException {
        try {
            JSONArray schemaJson = (JSONArray) parser.parse(new InputStreamReader(
                    JanaSchema.class.getClassLoader().getResourceAsStream(DROP_SCHEMA_FILE)));
            connectionManager.createRelations(schemaJson);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error While Creating Schema");
        }
    }
}
