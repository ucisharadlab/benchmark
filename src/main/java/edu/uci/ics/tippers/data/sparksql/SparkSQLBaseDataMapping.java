package edu.uci.ics.tippers.data.sparksql;

import java.sql.Connection;

public abstract class SparkSQLBaseDataMapping {

    protected Connection connection;
    protected String dataDir;

    public SparkSQLBaseDataMapping(Connection connection, String dataDir) {
        this.connection = connection;
        this.dataDir = dataDir;
    }

    public abstract void addAll();

    public abstract void insertPerformance();

}
