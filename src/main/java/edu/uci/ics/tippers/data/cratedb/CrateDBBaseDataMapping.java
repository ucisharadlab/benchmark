package edu.uci.ics.tippers.data.cratedb;

import java.sql.Connection;

public abstract class CrateDBBaseDataMapping {

    protected Connection connection;
    protected String dataDir;

    public CrateDBBaseDataMapping(Connection connection, String dataDir) {
        this.connection = connection;
        this.dataDir = dataDir;
    }

    public abstract void addAll();

    public abstract void insertPerformance();

}
