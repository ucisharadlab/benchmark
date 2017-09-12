package edu.uci.ics.tippers.schema.asterixdb;

import edu.uci.ics.tippers.connection.asterixdb.ConnectionManager;
import edu.uci.ics.tippers.schema.BaseSchema;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;

public class AsterixDBSchema extends BaseSchema {

    private String SCHEMA_FILE = "/asterixdb/schema/mapping1.sqlpp";

    @Override
    public void createSchema() {
        InputStream inputStream = AsterixDBSchema.class.getClassLoader().getResourceAsStream(SCHEMA_FILE);
        String queryString = null;
        try {
            queryString = IOUtils.toString(inputStream, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

        HttpResponse response = ConnectionManager.getInstance().sendQuery(queryString);
        try {
            System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void dropSchema() {

    }

}
