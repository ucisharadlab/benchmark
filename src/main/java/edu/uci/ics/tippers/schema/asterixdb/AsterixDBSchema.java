package edu.uci.ics.tippers.schema.asterixdb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import edu.uci.ics.tippers.schema.BaseSchema;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;

public class AsterixDBSchema extends BaseSchema {

    private String CREATE_SCHEMA_FILE = "asterixdb/schema/mapping1/create.sqlpp";
    private String DROP_SCHEMA_FILE = "asterixdb/schema/mapping1/drop.sqlpp";

    public AsterixDBSchema(int mapping) {
        super(mapping);
    }

    @Override
    public Database getDatabase() {
        return null;
    }

    private  void runSQLPPFile(String file) {
        InputStream inputStream = AsterixDBSchema.class.getClassLoader().getResourceAsStream(file);
        String queryString = null;
        try {
            queryString = IOUtils.toString(inputStream, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

        HttpResponse response = AsterixDBConnectionManager.getInstance().sendQuery(queryString, false);
        try {
            System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createSchema() {
        runSQLPPFile(CREATE_SCHEMA_FILE);
    }

    @Override
    public void dropSchema() {
        runSQLPPFile(DROP_SCHEMA_FILE);
    }

}
