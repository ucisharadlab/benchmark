package edu.uci.ics.tippers.connection.griddb;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by peeyush on 13/10/16.
 */
public class StoreManager {

    private static final Logger LOGGER = Logger.getLogger(StoreManager.class);
    private static StoreManager _instance = new StoreManager();
    private Properties props;
    private GridStoreFactory factory;
    private static int count = 0;

    private StoreManager() {
        // Get a GridStore instance
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("griddb/griddb.properties");
            props = new Properties();
            props.load(inputStream);
            Properties factoryProperties = new Properties();
            factoryProperties.put("maxConnectionPoolSize", 10000);
            factory = GridStoreFactory.getInstance();
            factory.setProperties(factoryProperties);

        } catch (IOException ie) {
            LOGGER.error(ie);
        }
    }

    void incrementSync() {
        synchronized (this) {
            count = count + 1;
        }
    }

    private  void  decrementSync() {
        synchronized (this) {
            count = count - 1;
        }
    }



    public  GridStore getGridStore() {
        GridStore gridStore = null;
        try {
            gridStore = factory.getGridStore(props);
        } catch (GSException e) {
            e.printStackTrace();
        }
        return gridStore;

    }

    public static  StoreManager getInstance(){

            return _instance;

    }
}


