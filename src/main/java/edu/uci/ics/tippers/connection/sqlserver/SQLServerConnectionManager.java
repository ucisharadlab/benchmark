package edu.uci.ics.tippers.connection.sqlserver;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.BaseConnectionManager;
import edu.uci.ics.tippers.connection.postgresql.PgSQLConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class SQLServerConnectionManager extends BaseConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(SQLServerConnectionManager.class);
    private static SQLServerConnectionManager _instance = new SQLServerConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String DATABASE;
    private static String USER;
    private static String PASSWORD;

    private SQLServerConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("sqlserver/sqlserver.properties");
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

    public static SQLServerConnectionManager getInstance() {
        return _instance;
    }


    public Connection getConnection() throws BenchmarkException {
        try {

            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        } catch (ClassNotFoundException e) {

            System.out.println("Where is your SQL Server JDBC Driver? "
                    + "Include in your library path!");
            e.printStackTrace();

        }

        Connection connection;
        try {
            connection = DriverManager.getConnection(
                    String.format("jdbc:sqlserver://%s:%s;databaseName=%s", SERVER, PORT, DATABASE), USER, PASSWORD);

            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Connecting to SQL Server");
        }
    }

    public Connection getEncryptedConnection() throws BenchmarkException {
        try {

            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        } catch (ClassNotFoundException e) {

            System.out.println("Where is your SQL Server JDBC Driver? "
                    + "Include in your library path!");
            e.printStackTrace();

        }

        Connection connection;
        try {
            connection = DriverManager.getConnection(
                    String.format("jdbc:sqlserver://%s:%s;databaseName=%s;columnEncryptionSetting=Enabled;" +
                                    "keyStoreAuthentication=JavaKeyStorePassword;" +
                                    "keyStoreLocation=/home/sgx_admin/keystore.jks;keyStoreSecret=mypassword;",
                            SERVER, PORT, DATABASE), USER, PASSWORD);

            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Connecting to SQL Server");
        }
    }


    @Override
    public JSONArray runQueryWithJSONResults(String query) {
        return null;
    }
}
