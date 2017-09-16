package edu.uci.ics.tippers.data.griddb;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;

public abstract class BaseDataMapping {

    protected GridStore gridStore;
    private String dataDir;

    public BaseDataMapping(GridStore gridStore, String dataDir) {
        this.gridStore = gridStore;
        this.dataDir = dataDir;
    }

    public abstract void addAll() throws GSException;

}
