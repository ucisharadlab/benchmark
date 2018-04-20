package edu.uci.ics.tippers.operators;

import edu.uci.ics.tippers.wrapper.griddb.translator.Operator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.List;

/**
 * Created by peeyush on 18/5/17.
 */
public class AggregateOperator {

    public  static Object sum(String field, JSONArray list) throws JSONException {

        Double sum=0.0;
        for (int i=0; i< list.length(); i++){
            JSONObject currentJsonObject = list.getJSONObject(i);
            sum+=(double)Operator.pathNavigation(currentJsonObject,field, true);
        }

        return sum;
    }

    public  static Object count(JSONArray list) {

        return list.length();
    }

    public static JSONArray count(JSONArray list, List<Integer> fields) {

        JSONArray results = new JSONArray();
        for(int i=0; i<list.length(); i++) {
            JSONArray row = new JSONArray();
            for(int j=0; j<fields.size(); j++) {
                row.put(list.getJSONArray(i).getJSONArray(0).getString(fields.get(j)));
            }
            row.put(list.getJSONArray(i).length());
            results.put(row);
        }
        return results;
    }

    public static Object avg(String field, JSONArray list) throws JSONException {
        Long sum=0l;
        for (int i=0; i< list.length(); i++){
            JSONObject currentJsonObject = list.getJSONObject(i);
            sum+=(Long) Operator.pathNavigation(currentJsonObject,field, true);
        }

        return sum/list.length();
    }

    public static Object max(String field, JSONArray list) throws JSONException {
        Double tmp,max;
        max=-1.0;
        for (int i=0; i< list.length(); i++){
            JSONObject currentJsonObject = list.getJSONObject(i);
            tmp=(double)Operator.pathNavigation(currentJsonObject,field, true);
            if(tmp>max)
                max=tmp;
        }

        return max;
    }

    public static Object min(String field, JSONArray list) throws JSONException {
        Long tmp=(Long)Operator.pathNavigation(list.getJSONObject(0),field, true);
        Long min=tmp;
        for (int i=1; i< list.length(); i++){
            JSONObject currentJsonObject = list.getJSONObject(i);
            tmp=(Long)Operator.pathNavigation(currentJsonObject,field, true);
            if(tmp<min)
                min=tmp;
        }

        return min;
    }

}
