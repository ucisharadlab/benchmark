package edu.uci.ics.tippers.data.griddb;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;

public abstract class GridDBBaseDataMapping {

    protected GridStore gridStore;
    protected String dataDir;

    public GridDBBaseDataMapping(GridStore gridStore, String dataDir) {
        this.gridStore = gridStore;
        this.dataDir = dataDir;
    }

    public abstract void addAll() throws GSException;

}
