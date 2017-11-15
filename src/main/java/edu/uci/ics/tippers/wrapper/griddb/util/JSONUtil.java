package edu.uci.ics.tippers.wrapper.griddb.util;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by peeyush on 23/2/17.
 */
public class JSONUtil {
    public static String createMessageJson(String message) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("message", message);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }
}
