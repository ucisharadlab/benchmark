package edu.uci.ics.tippers.connection.mongodb;

import com.mongodb.DB;
import com.mongodb.MongoClient;

/**
 * Created by peeyush on 19/9/17.
 */

public class DBManager {
    private static DBManager instance;
    MongoClient mongoClient;
    DB db;

//    private DBManager(){
//
//        mongoClient = new MongoClient(config.DBPath,config.DBPort);
//        db = mongoClient.getDB("sushi-sensors");
//    }
//
//    public DB getDb() {
//        return db;
//    }
//
//    public void setDb(DB db) {
//        this.db = db;
//    }
//
//    public static DBManager getInstance(){
//
//        if(instance == null)
//            instance = new DBManager();
//        return instance;
//    }

}
