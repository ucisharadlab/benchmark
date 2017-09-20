package edu.uci.ics.tippers.connection.postgresql;

import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PgSQLConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(AsterixDBConnectionManager.class);
    private static PgSQLConnectionManager _instance = new PgSQLConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String DATABASE;
    private static String USER;
    private static String PASSWORD;

    private PgSQLConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("postgresql/pgsql.properties");
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

    public static PgSQLConnectionManager getInstance() {
        return _instance;
    }


    public Connection getConnection() throws BenchmarkException {
        try {

            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException e) {

            System.out.println("Where is your PostgreSQL JDBC Driver? "
                    + "Include in your library path!");
            e.printStackTrace();

        }

        Connection connection;
        try {
            connection = DriverManager.getConnection(
                    String.format("jdbc:postgresql://%s:%s/%s", SERVER, PORT, DATABASE), USER, PASSWORD);

            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Connecting to Postgres");
        }
    }

}
