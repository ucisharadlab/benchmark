package edu.uci.ics.tippers.model.virtualSensor;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by peeyush on 6/4/17.
 */
public class VirtualSensorConfig extends JSONObject {
    private String config;

    public VirtualSensorConfig(String payload) throws JSONException {
        super(payload);
    }

    public VirtualSensorConfig() {
        super();
    }

    public String getConfig() {
        return super.toString();
    }

    public void setConfig(String config) {
        this.config = config;
    }
}
