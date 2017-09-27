package edu.uci.ics.tippers.operators;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by peeyush on 10/8/17.
 */
public class GroupBy {

    public GroupBy() {

    }

    private List<Object> prepareKeyObject(JSONObject jsonObject, List<String> fields) throws JSONException {
        List<Object>  result = new ArrayList<>();
        for (String field: fields) {
            result.add(jsonObject.get(field));
        }
        return result;
    }

    public JSONArray doGroupBy(JSONArray currentCollection, List<String> fields) throws JSONException {
        JSONArray jsonArray = new JSONArray();
        Map<List<Object>, JSONArray> groups = new HashMap<>();

        for (int j=0; j<currentCollection.length(); j++){
            JSONObject outerJsonObject = currentCollection.getJSONObject(j);
            List<Object>  keyObject = prepareKeyObject(outerJsonObject, fields);

            if (!groups.containsKey(keyObject)){
                groups.put(keyObject, new JSONArray());
            }
            groups.get(keyObject).put(outerJsonObject);

        }
        for(List<Object>  object:groups.keySet())
            jsonArray.put(groups.get(object));

        return jsonArray;
    }
}
