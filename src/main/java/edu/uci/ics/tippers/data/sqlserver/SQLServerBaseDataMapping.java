package edu.uci.ics.tippers.data.sqlserver;

import java.sql.Connection;

public abstract class SQLServerBaseDataMapping {

    protected Connection connection;
    protected String dataDir;

    public SQLServerBaseDataMapping(Connection connection, String dataDir) {
        this.connection = connection;
        this.dataDir = dataDir;
    }

    public abstract void addAll();

    public abstract void insertPerformance();

}
