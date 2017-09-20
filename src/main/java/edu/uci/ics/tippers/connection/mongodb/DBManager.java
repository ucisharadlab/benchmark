package edu.uci.ics.tippers.connection.mongodb;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by peeyush on 19/9/17.
 */

public class DBManager {

    private Properties props;
    private static DBManager instance;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private static String SERVER;
    private static int PORT;
    private static String DATABASE;

    private DBManager(){
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("mongodb/mongodb.properties");
            props = new Properties();
            props.load(inputStream);

            SERVER = props.getProperty("server");
            PORT = Integer.parseInt(props.getProperty("port"));
            DATABASE = props.getProperty("database");

        } catch (IOException ie) {
            ie.printStackTrace();
        }

        mongoClient = new MongoClient(SERVER, PORT);
        database = mongoClient.getDatabase(DATABASE);
    }

    public MongoDatabase getDatabase() {
        return database;
    }

    public void setDatabase(MongoDatabase database) {
        this.database = database;
    }

    public static DBManager getInstance(){

        if(instance == null)
            instance = new DBManager();
        return instance;
    }

    public void dropDatabase(){
        mongoClient.dropDatabase(DATABASE);
    }

}
