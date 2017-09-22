package edu.uci.ics.tippers.schema.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.cassandra.CassandraConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;

/**
 * Created by peeyush on 21/9/17.
 */
public class CassandraSchema extends BaseSchema {

    public CassandraSchema(int mapping, String dataDir) {
        super(mapping, dataDir);
        ResultSet rs = CassandraConnectionManager.getInstance().getSession()
                .execute("select release_version from system.local");    // (3)
        Row row = rs.one();
        System.out.println(row.getString("release_version"));
    }

    @Override
    public Database getDatabase() {
        return Database.CASSANDRA;
    }

    @Override
    public void createSchema() throws BenchmarkException {

    }

    @Override
    public void dropSchema() throws BenchmarkException {

    }
}
