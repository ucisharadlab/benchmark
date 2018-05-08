package edu.uci.ics.tippers.schema.couchbase;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import edu.uci.ics.tippers.connection.couchbase.CouchbaseConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;
import edu.uci.ics.tippers.schema.asterixdb.AsterixDBSchema;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.io.InputStream;

public class CouchbaseSchema extends BaseSchema{

    private String SCHEMA_FILE = "couchbase/schema.txt";
    private String CREATE_INDEX = "CREATE PRIMARY INDEX %s_index ON %s";

    public CouchbaseSchema(int mapping, String dataDir) {
        super(mapping, dataDir);
    }

    @Override
    public Database getDatabase() {
        return Database.COUCHBASE;
    }

    @Override
    public void createSchema() {
        InputStream inputStream = CouchbaseSchema.class.getClassLoader().getResourceAsStream(SCHEMA_FILE);
        String buckets[] = null;
        try {
            buckets = IOUtils.toString(inputStream, "UTF-8").split("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (String bucket : buckets) {
            HttpResponse response = CouchbaseConnectionManager.getInstance().createBucket(bucket);
            response = CouchbaseConnectionManager.getInstance().sendQuery(String.format(CREATE_INDEX, bucket, bucket));
        }
    }

    @Override
    public void dropSchema() {
        InputStream inputStream = CouchbaseSchema.class.getClassLoader().getResourceAsStream(SCHEMA_FILE);
        String buckets[] = null;
        try {
            buckets = IOUtils.toString(inputStream, "UTF-8").split("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (String bucket : buckets) {
            HttpResponse response = CouchbaseConnectionManager.getInstance().deleteBucket(bucket);
        }
    }
}
