package edu.uci.ics.tippers.connection.influxdb;

import edu.uci.ics.tippers.connection.BaseConnectionManager;
import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
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

    private InfluxDBConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("influxdb/influxdb.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = props.getProperty("port");
            DB = props.getProperty("db");

            // Warming Up
            sendQuery(";");

        } catch (IOException ie) {
            LOGGER.error(ie);
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
            //System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public HttpResponse write(String inserts) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/write?db=%s", SERVER, PORT, DB);
        HttpPost httpPost = new HttpPost(url);

        httpPost.setEntity(new StringEntity(inserts, "UTF-8"));

        CloseableHttpResponse response = null;
        try {
            response = client.execute(httpPost);
            //System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }


}
