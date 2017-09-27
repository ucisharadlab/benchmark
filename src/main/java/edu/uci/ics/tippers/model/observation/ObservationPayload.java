package edu.uci.ics.tippers.model.observation;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by peeyush on 6/4/17.
 */
public class ObservationPayload extends JSONObject {

    private String config;

    public ObservationPayload(String payload) throws JSONException {
        super(payload);
    }

    public ObservationPayload() {
        super();
    }

    public String getConfig() {
        return toString();
    }

    public void setConfig(String config) {
        this.config = config;
    }
}
