package edu.uci.ics.tippers.connection.influxdb;

import edu.uci.ics.tippers.connection.BaseConnectionManager;
import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
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

public class InfluxDBConnectionManager extends BaseConnectionManager {
    private static final Logger LOGGER = Logger.getLogger(InfluxDBConnectionManager.class);
    private static InfluxDBConnectionManager _instance = new InfluxDBConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String DB;

    private static String PG_SERVER;
    private static String PG_PORT;
    private static String PG_DATABASE;
    private static String PG_USER;
    private static String PG_PASSWORD;

    private InfluxDBConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("influxdb/influxdb.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = props.getProperty("port");
            DB = props.getProperty("db");

            PG_SERVER = props.getProperty("pg-server");
            PG_PORT = props.getProperty("pg-port");
            PG_DATABASE = props.getProperty("pg-database");
            PG_USER = props.getProperty("pg-user");
            PG_PASSWORD = props.getProperty("pg-password");

            // Warming Up
            //sendQuery(";");

        } catch (IOException ie) {
            LOGGER.error(ie);
        }
    }

    public Connection getMetadataConnection() throws BenchmarkException {
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
                    String.format("jdbc:postgresql://%s:%s/%s", PG_SERVER, PG_PORT, PG_DATABASE), PG_USER, PG_PASSWORD);

            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Connecting to Postgres");
        }
    }

    public static InfluxDBConnectionManager getInstance() {
        return _instance;
    }

    public HttpResponse addSchema() {
        return sendQuery(String.format("CREATE DATABASE %s", DB ));
    }

    public HttpResponse deleteSchema() {
        return sendQuery(String.format("DELETE DATABASE %s", DB ));
    }

    public HttpResponse sendQuery(String query) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/query", SERVER, PORT);
        HttpPost httpPost = new HttpPost(url);

        List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        params.add(new BasicNameValuePair("q", query));

        try {
            httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


        CloseableHttpResponse response = null;
        try {
            response = client.execute(httpPost);
//            if (response.getStatusLine().getStatusCode()!=204)
//                System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public HttpResponse write(String inserts) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/write?db=%s", SERVER, PORT, DB);
        HttpPost httpPost = new HttpPost(url);

        List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        params.add(new BasicNameValuePair("precision", "ms"));
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        httpPost.setEntity(new StringEntity(inserts, "UTF-8"));

        CloseableHttpResponse response = null;
        try {
            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode()!=204)
                System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }


}
