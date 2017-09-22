package edu.uci.ics.tippers.connection.cratedb;

import edu.uci.ics.tippers.exception.BenchmarkException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class CrateDBConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(CrateDBConnectionManager.class);
    private static CrateDBConnectionManager _instance = new CrateDBConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String DATABASE;
    private static String USER;
    private static String PASSWORD;

    private CrateDBConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("cratedb/cratedb.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = props.getProperty("port");
            DATABASE = props.getProperty("database");
            USER = props.getProperty("user");
            PASSWORD = props.getProperty("password");

        } catch (IOException ie) {
            LOGGER.error(ie);
        }
    }

    public static CrateDBConnectionManager getInstance() {
        return _instance;
    }


    public Connection getConnection() throws BenchmarkException {

        Connection connection;
        try {
            Properties properties = new Properties();
            properties.put("user", USER);
            connection = DriverManager.getConnection(
                    String.format("jdbc:crate://%s:%s/", SERVER, PORT), properties);
            connection.setSchema(DATABASE);

            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Connecting to CrateDB");
        }
    }

}
