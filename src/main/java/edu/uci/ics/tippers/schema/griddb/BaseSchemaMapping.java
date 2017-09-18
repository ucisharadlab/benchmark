package edu.uci.ics.tippers.schema.griddb;

import com.toshiba.mwcloud.gs.*;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Created by peeyush on 26/4/17.
 */
public abstract class BaseSchemaMapping {

    protected GridStore gridStore;
    protected String dataDir;

    public BaseSchemaMapping(GridStore gridStore, String dataDir) {
        this.gridStore = gridStore;
        this.dataDir = dataDir;
    }

    public abstract void createAll() throws GSException;

    public abstract void deleteAll() throws GSException;

}
