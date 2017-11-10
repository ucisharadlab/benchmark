package edu.uci.ics.tippers.data.asterixdb;

import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import org.apache.commons.text.StringEscapeUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

public class AsterixDataFeed {

    private String START_FEED_STMT = "start feed %sFeed;";
    private String STOP_FEED_STMT = "stop feed %sFeed;";
    private String collectionName;
    private AsterixDBConnectionManager connectionManager;
    private Socket client;
    private DataOutputStream outputStream;


    public AsterixDataFeed(String collectionName, AsterixDBConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        this.collectionName = collectionName;
        startFeed();
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        createSocketConnection();
    }

    public void createSocketConnection() {

        try {
            client = new Socket(Constants.ASTERIX_FEED_IP, Constants.ASTERIX_FEED_PORT);
            outputStream = new DataOutputStream(client.getOutputStream());
        }
        catch (IOException e) {
            System.out.println(e);
        }
    }

    public void startFeed() {
        connectionManager.sendQuery(String.format(START_FEED_STMT, collectionName));
    }

    public void sendDataToFeed(String line) {
        try {
            outputStream.writeBytes( StringEscapeUtils.unescapeJava(line));
            outputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stopFeed() {
        try {
            outputStream.close();
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        connectionManager.sendQuery(String.format(STOP_FEED_STMT, collectionName, collectionName, collectionName));

    }

}
