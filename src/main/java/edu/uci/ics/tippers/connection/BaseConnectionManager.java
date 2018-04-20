package edu.uci.ics.tippers.connection;

import org.json.JSONArray;

public abstract class BaseConnectionManager {

    public abstract JSONArray runQueryWithJSONResults(String query);

}
