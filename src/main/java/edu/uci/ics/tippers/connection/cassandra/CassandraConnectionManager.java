package edu.uci.ics.tippers.connection.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by peeyush on 21/9/17.
 */
public class CassandraConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(CassandraConnectionManager.class);
    private static CassandraConnectionManager _instance = new CassandraConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String DATAVERSE;
    private static Session session;

    private CassandraConnectionManager() {
        Cluster cluster = null;
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("cassandra/cassandra.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = props.getProperty("port");
            DATAVERSE = props.getProperty("dataverse");

            cluster = Cluster.builder()
                    .addContactPoint(SERVER)
                    .build();
            session = cluster.connect();

        } catch (IOException ie) {
            LOGGER.error(ie);
        }
    }

    public static CassandraConnectionManager getInstance() {
        return _instance;
    }

    public Session getSession() {
        return session;
    }

}
