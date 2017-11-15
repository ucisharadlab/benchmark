package edu.uci.ics.tippers.wrapper.griddb.translator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Iterator;

/**
 * Created by peeyush on 3/5/17.
 */
public class Join {

    public Join() {

    }

    public static JSONArray loopJoin(JSONArray finalCollection, JSONArray currentCollection, String alias) throws JSONException {
        JSONArray jsonArray = new JSONArray();

        for (int j=0; j<finalCollection.length(); j++){
            JSONObject outerJsonObject = finalCollection.getJSONObject(j);

            for (int k=0; k<currentCollection.length(); k++) {
                JSONObject innerJsonObject = currentCollection.getJSONObject(k);
                JSONObject finalJsonObject = new JSONObject(outerJsonObject.toString());

                Iterator<String> keys = innerJsonObject.keys();
                while( keys.hasNext() ) {
                    String key = keys.next();
                    finalJsonObject.put(alias+"."+key, innerJsonObject.get(key));
                }
                jsonArray.put(finalJsonObject);
            }
        }
        return jsonArray;
    }

}
