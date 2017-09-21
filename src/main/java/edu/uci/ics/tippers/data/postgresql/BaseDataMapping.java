package edu.uci.ics.tippers.data.postgresql;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;

import java.sql.Connection;

public abstract class BaseDataMapping {

    protected Connection connection;
    protected String dataDir;

    public BaseDataMapping(Connection connection, String dataDir) {
        this.connection = connection;
        this.dataDir = dataDir;
    }

    public abstract void addAll();

}
