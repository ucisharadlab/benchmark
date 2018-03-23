package edu.uci.ics.tippers.connection.jana;

import edu.uci.ics.tippers.connection.BaseConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

public class JanaConnectionManager extends BaseConnectionManager {
    private static final Logger LOGGER = Logger.getLogger(JanaConnectionManager.class);
    private static JanaConnectionManager _instance = new JanaConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String DDL_PORT;
    private static String DML_PORT;

    private JanaConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("jana/jana.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            DML_PORT = props.getProperty("dml-port");
            DDL_PORT = props.getProperty("ddl-port");

            // Warming Up
            //sendQuery(";");

        } catch (IOException ie) {
            LOGGER.error(ie);
        }
    }

    public static JanaConnectionManager getInstance() {
        return _instance;
    }

    public HttpResponse doInsert(String relation, JSONArray rows) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/insert", SERVER, DML_PORT);
        HttpPost httpPost = new HttpPost(url);


        JSONObject payload = new JSONObject();
        payload.put("relation", relation);
        payload.put("rows", rows);

        try {
            httpPost.addHeader("content-type", "application/json");
            httpPost.setEntity(new StringEntity(payload.toString()));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        CloseableHttpResponse response = null;
        try {
            response = client.execute(httpPost);
            //System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error While Inserting Data");
        }

        if (response.getStatusLine().getStatusCode() != 200) {
            try {
                System.out.println(EntityUtils.toString(response.getEntity()));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                throw new BenchmarkException("Error Running Query");
            }
        }
        return response;
    }

    public HttpResponse createRelations(JSONArray relations) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/create", SERVER, DDL_PORT);
        HttpPost httpPost = new HttpPost(url);

        JSONObject payload = new JSONObject();
        payload.put("relations", relations);

        try {
            httpPost.addHeader("content-type", "application/json");
            httpPost.setEntity(new StringEntity(payload.toString()));
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

    public HttpResponse dropRelations(JSONArray relations) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/drop", SERVER, DDL_PORT);
        HttpPost httpPost = new HttpPost(url);

        JSONObject payload = new JSONObject();
        payload.put("relations", relations);

        try {
            httpPost.addHeader("content-type", "application/json");
            httpPost.setEntity(new StringEntity(payload.toString()));
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

    public HttpResponse sendQuery(String query) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/query", SERVER, DML_PORT);
        HttpPost httpPost = new HttpPost(url);


        JSONObject payload = new JSONObject();
        payload.put("query", query);

        try {
            httpPost.addHeader("content-type", "application/json");
            httpPost.setEntity(new StringEntity(payload.toString()));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


        CloseableHttpResponse response = null;
        try {
            response = client.execute(httpPost);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (response.getStatusLine().getStatusCode() != 200) {
            try {
                System.out.println(EntityUtils.toString(response.getEntity()));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                throw new BenchmarkException("Error Running Query");
            }
        }
        return response;
    }
}
