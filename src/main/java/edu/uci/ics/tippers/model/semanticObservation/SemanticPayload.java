package edu.uci.ics.tippers.model.semanticObservation;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by peeyush on 6/4/17.
 */
public class SemanticPayload extends JSONObject {

    public SemanticPayload(String payload) throws JSONException {
        super(payload);
    }

    public SemanticPayload() {
        super();
    }

}
