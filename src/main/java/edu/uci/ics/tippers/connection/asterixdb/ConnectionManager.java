package edu.uci.ics.tippers.connection.asterixdb;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
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

/**
 * Created by peeyush on 29/8/17.
 */
public class ConnectionManager {
    private static final Logger LOGGER = Logger.getLogger(ConnectionManager.class);
    private static ConnectionManager _instance = new ConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String DATAVERSE;

    private ConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("/asterixdb/asterixdb.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = props.getProperty("port");
            DATAVERSE = props.getProperty("dataverse");

        } catch (IOException ie) {
            LOGGER.error(ie);
        }
    }

    public static ConnectionManager getInstance() {
        return _instance;
    }

    public HttpResponse sendQuery(String query) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/query/service", SERVER, PORT);
        HttpPost httpPost = new HttpPost(url);
        query = String.format("Use %s; ", DATAVERSE) + query;

        List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        params.add(new BasicNameValuePair("statement", query));
//        params.add(new BasicNameValuePair("query-language", "SQLPP"));
//        params.add(new BasicNameValuePair("output-format", "CLEAN_JSON"));
//        params.add(new BasicNameValuePair("execute-query", "true"));

        try {
            httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        CloseableHttpResponse response = null;
        try {
            response = client.execute(httpPost);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

}
