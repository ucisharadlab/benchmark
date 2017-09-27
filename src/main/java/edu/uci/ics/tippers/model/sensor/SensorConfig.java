package edu.uci.ics.tippers.model.sensor;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by peeyush on 6/4/17.
 */
public class SensorConfig extends JSONObject {

    private String config;

    public SensorConfig(String payload) throws JSONException {
        super(payload);
    }

    public SensorConfig() {
        super();
    }

    public String getConfig() {
        return super.toString();
    }

    public void setConfig(String config) {
        this.config = config;
    }
}
