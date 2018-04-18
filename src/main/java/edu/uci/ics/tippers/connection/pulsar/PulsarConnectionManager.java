package edu.uci.ics.tippers.connection.pulsar;

import edu.uci.ics.tippers.common.util.Helper;
import edu.uci.ics.tippers.connection.BaseConnectionManager;
import edu.uci.ics.tippers.connection.jana.JanaConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PulsarConnectionManager  extends BaseConnectionManager{

    private static final Logger LOGGER = Logger.getLogger(PulsarConnectionManager.class);
    private static PulsarConnectionManager _instance = new PulsarConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;

    private PulsarConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("pulsar/pulsar.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = props.getProperty("port");

            // Warming Up
            //sendQuery(";");

        } catch (IOException ie) {
            LOGGER.error(ie);
        }
    }

    public static PulsarConnectionManager getInstance() {
        return _instance;
    }

    public HttpResponse ingest(String createRelation, String relation, List<List<String>> rows) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/ingest", SERVER, PORT);
        HttpPost httpPost = new HttpPost(url);

        List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        params.add(new BasicNameValuePair(createRelation, ""));

        try {
            httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            httpPost.setEntity(new StringEntity(Helper.listToInsertString(relation, rows)));
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


    public HttpResponse sendQuery(String query) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/query", SERVER, PORT);
        HttpPost httpPost = new HttpPost(url);


        List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        params.add(new BasicNameValuePair(query, ""));

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
