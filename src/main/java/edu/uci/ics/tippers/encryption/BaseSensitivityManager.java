package edu.uci.ics.tippers.encryption;

import org.json.simple.JSONObject;

public abstract class BaseSensitivityManager {

    public abstract Boolean checkUserSensitive(String userId);

    public abstract Boolean checkSensorSensitive(String sensorId);

    public abstract Boolean checkObservationSensitive(JSONObject observation);

}
