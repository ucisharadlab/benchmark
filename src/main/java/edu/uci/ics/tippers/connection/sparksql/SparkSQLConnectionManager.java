package edu.uci.ics.tippers.connection.sparksql;

import edu.uci.ics.tippers.exception.BenchmarkException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class SparkSQLConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(SparkSQLConnectionManager.class);
    private static SparkSQLConnectionManager _instance = new SparkSQLConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String DATABASE;
    private static String USER;
    private static String PASSWORD;

    private SparkSQLConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("sparksql/sparksql.properties");
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

    public static SparkSQLConnectionManager getInstance() {
        return _instance;
    }


    public Connection getConnection() throws BenchmarkException {

        Connection connection;
        try {
            Properties properties = new Properties();
            properties.put("user", USER);
            connection = DriverManager.getConnection(
                    String.format("jdbc:hive2://%s:%s/", SERVER, PORT), properties);
            connection.setSchema(DATABASE);

            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Connecting to SparkSQL Thrift Server");
        }
    }

}
