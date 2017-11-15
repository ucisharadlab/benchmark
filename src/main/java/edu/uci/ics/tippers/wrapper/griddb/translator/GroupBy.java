package edu.uci.ics.tippers.wrapper.griddb.translator;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.clause.GroupbyClause;
import edu.uci.ics.tippers.tql.lang.common.expression.FieldAccessor;
import edu.uci.ics.tippers.tql.lang.common.expression.VariableExpr;
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

    public JSONArray doGroupBy(JSONArray currentCollection, GroupbyClause groupbyClause) throws JSONException {
        List<Expression> expressionList = groupbyClause.getExpressions();
        List<String> fields = new ArrayList<>();
        for (Expression expression : expressionList) {
            if (expression.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
               fields.add(((VariableExpr) expression).getVar().getValue());

            }else if (expression.getKind() == Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
                fields.add(((FieldAccessor) expression).toString());
            }
        }
        return  doGroupBy(currentCollection, fields);
    }

    private List<Object> prepareKeyObject(JSONObject jsonObject, List<String> fields) throws JSONException {
        List<Object>  result = new ArrayList<>();
        for (String field: fields) {
            result.add(Operator.pathNavigation((JSONObject) jsonObject, field, false));
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
