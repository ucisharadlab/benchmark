package edu.uci.ics.tippers.connection.couchbase;

import edu.uci.ics.tippers.common.util.Helper;
import edu.uci.ics.tippers.connection.BaseConnectionManager;
import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CouchbaseConnectionManager extends BaseConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(CouchbaseConnectionManager.class);
    private static CouchbaseConnectionManager _instance = new CouchbaseConnectionManager();
    private Properties props;
    private static String SERVER;
    private static String PORT;
    private static String USER;
    private static String PASSWORD;
    private static String QUERY_PORT;

    private CouchbaseConnectionManager() {
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("couchbase/couchbase.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = props.getProperty("port");
            USER = props.getProperty("user");
            PASSWORD = props.getProperty("password");
            QUERY_PORT = props.getProperty("query-port");

            // Warming Up
            //sendQuery(";");

        } catch (IOException ie) {
            LOGGER.error(ie);
        }
    }

    public static CouchbaseConnectionManager getInstance() {
        return _instance;
    }

    public HttpResponse sendQuery(String query) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/query/service", SERVER, QUERY_PORT);
        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("Authorization", "Basic " + Helper.createAuthentication(USER, PASSWORD));


        List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        params.add(new BasicNameValuePair("statement", query));

        try {
            httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


        CloseableHttpResponse response = null;
        try {
            response = client.execute(httpPost);
            System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public HttpResponse createBucket(String bucket) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/pools/default/buckets", SERVER, PORT);
        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("Authorization", "Basic " + Helper.createAuthentication(USER, PASSWORD));

        List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        params.add(new BasicNameValuePair("name", bucket));
        params.add(new BasicNameValuePair("authType", "sasl"));
        params.add(new BasicNameValuePair("saslPassword", "pass"));
        params.add(new BasicNameValuePair("bucketType", "couchbase"));
        params.add(new BasicNameValuePair("ramQuotaMB", "100"));


        try {
            httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        CloseableHttpResponse response = null;
        try {
            response = client.execute(httpPost);
            System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public HttpResponse deleteBucket(String bucket) {
        CloseableHttpClient client = HttpClients.createDefault();
        String url = String.format("http://%s:%s/pools/default/buckets/%s", SERVER, PORT, bucket);
        HttpDelete httpDelete = new HttpDelete(url);
        httpDelete.setHeader("Authorization", "Basic " + Helper.createAuthentication(USER, PASSWORD));


        CloseableHttpResponse response = null;
        try {
            response = client.execute(httpDelete);
            System.out.println(EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }


}
