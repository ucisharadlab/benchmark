package edu.uci.ics.tippers.operators;

import edu.uci.ics.tippers.common.DataType;
import edu.uci.ics.tippers.connection.BaseConnectionManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.xml.crypto.Data;
import java.util.Iterator;

/**
 * Created by peeyush on 3/5/17.
 */
public class Join {

    public Join() {

    }

    public static JSONArray nestedLoopJoin(JSONArray finalCollection, JSONArray currentCollection, String alias) throws JSONException {
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

    public static JSONArray indexLoopJoin(JSONArray leftRelation, String rightRelationQuery, int keyIndex,
                                          DataType dataType, BaseConnectionManager connectionManager) throws JSONException {

        JSONArray jsonArray = new JSONArray();
        String query = null;

        for (int j=0; j<leftRelation.length(); j++){
            switch (dataType) {
                case STRING:
                case DATETIME:
                    query = String.format(rightRelationQuery, leftRelation.getJSONArray(j).getString(keyIndex));
                    break;
                case INTEGER:
                    query = String.format(rightRelationQuery, leftRelation.getJSONArray(j).getInt(keyIndex));
                    break;
                case FLOAT:
                    query = String.format(rightRelationQuery, leftRelation.getJSONArray(j).getDouble(keyIndex));
                    break;
            }
            JSONArray returnedRow = connectionManager.runQueryWithJSONResults(query);
            for (int i=0; i<returnedRow.length(); i++) {
                JSONArray joinedRow = new JSONArray(leftRelation.getJSONArray(j).toString());
                for (int k = 0; k < returnedRow.getJSONArray(i).length(); k++) {
                    joinedRow.put(returnedRow.getJSONArray(i).getString(k));
                }
                jsonArray.put(joinedRow);
            }
        }
        return jsonArray;
    }

}
