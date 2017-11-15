package edu.uci.ics.tippers.wrapper.griddb.translator;

import com.toshiba.mwcloud.gs.GSException;
import edu.uci.ics.tippers.common.exceptions.CompilationException;
import edu.uci.ics.tippers.wrapper.griddb.util.GridDBCollections;
import edu.uci.ics.tippers.wrapper.griddb.util.GridDBManager;
import edu.uci.ics.tippers.wrapper.griddb.util.Helper;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.clause.GroupbyClause;
import edu.uci.ics.tippers.tql.lang.common.clause.LimitClause;
import edu.uci.ics.tippers.tql.lang.common.clause.WhereClause;
import edu.uci.ics.tippers.tql.lang.common.expression.*;
import edu.uci.ics.tippers.tql.lang.common.statement.Query;
import edu.uci.ics.tippers.tql.lang.common.struct.OperatorType;
import edu.uci.ics.tippers.tql.lang.sqlpp.clause.*;
import edu.uci.ics.tippers.tql.lang.sqlpp.expression.SelectExpression;
import edu.uci.ics.tippers.tql.parser.SQLPPParser;
import edu.uci.ics.tippers.tql.parser.SqlppParserFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by peeyush on 25/4/17.
 */
public class Translator {

    public static String SENSOR_COLLECTION = "Sensor";
    public static String OBSERVATION_COLLECTION = "Observation";

    public static String SO_TYPE_COLLECTION = "SemanticObservationType";
    public static String SO_COLLECTION = "SemanticObservation";

    private GridDBManager gridDBManager;
    private Query query = null;
    private Map<String, JSONArray> namedCollections = new HashMap<>();

    public Translator() {

    }

    public Translator(String query) throws CompilationException {

        gridDBManager = new GridDBManager();
        SqlppParserFactory factory = new SqlppParserFactory();
        SQLPPParser parser = factory.createParser(query);
        this.query = (Query)(parser.parse().get(0));
    }

    public Translator(Query query, Map<String, JSONArray> namedCollections) throws CompilationException {

        this.namedCollections = namedCollections;
        gridDBManager = new GridDBManager();
        this.query = query;
    }

    public JSONArray handleSelectClause(SelectClause selectClause, JSONArray collection, JSONObject outerScope)
            throws GSException, JSONException {

        List<Projection> projections = selectClause.getSelectRegular().getProjections();
        if (projections.size() == 1) {
            Expression expression = projections.get(0).getExpression();
            if (expression.getKind() == Expression.Kind.VARIABLE_EXPRESSION &&
                    ((VariableExpr) expression).getVar().getValue().equalsIgnoreCase("ALL"))
                return collection;
            else if (expression.getKind() == Expression.Kind.CALL_EXPRESSION){
                CallExpr callExpression = ((CallExpr)expression);
                String funcName = callExpression.getFunctionName().toLowerCase();
                String field = callExpression.getExprList().get(0).toString();

                JSONArray aggCollection = new JSONArray();
                JSONObject json = new JSONObject();

                Object result = ExpressionEvaluator.evaluateAggregateExpression(field, funcName, collection);

                if (funcName.equalsIgnoreCase("count"))
                    json.put(String.format("%s(*)", funcName), result);
                else
                    json.put(String.format("%s(%s)", funcName, field), result);
                aggCollection.put(json);

                return  aggCollection;
            }
        }


        JSONArray finalCollection = new JSONArray();
        for (int i=0; i< collection.length(); i++) {
            Object currentJsonObject = collection.get(i);
            JSONObject jsonObject = new JSONObject();

            for(Projection projection: projections) {
                Expression expression = projection.getExpression();
                String alias = null;
                if (expression.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
                    if (projection.getName() == null)
                        alias = ((VariableExpr) expression).getVar().getValue();
                    else
                        alias = projection.getName();
                    if (currentJsonObject instanceof JSONObject)
                        jsonObject.put(alias, Operator.pathNavigation((JSONObject) currentJsonObject,
                                ((VariableExpr) expression).getVar().getValue(), false));
                    else
                        jsonObject.put(alias, Operator.pathNavigation(((JSONArray) currentJsonObject).getJSONObject(0),
                                ((VariableExpr) expression).getVar().getValue(), false));
                } else if (expression.getKind() == Expression.Kind.SELECT_EXPRESSION){
                    alias = projection.getName();
                    if (currentJsonObject instanceof JSONObject)
                        outerScope = Helper.appendAttributes(outerScope, collection.getJSONObject(i));
                    else
                        outerScope = Helper.appendAttributes(outerScope, ((JSONArray)collection.get(i)).getJSONObject(0));
                    jsonObject.put(alias, fetchCollection(Helper.getSelectBlock((SelectExpression) expression),
                            outerScope));
                } else if (expression.getKind() == Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
                    if (projection.getName() == null)
                        alias = ((FieldAccessor) expression).toString();
                    else
                        alias = projection.getName();
                    if (currentJsonObject instanceof JSONObject)
                        jsonObject.put(alias, Operator.pathNavigation((JSONObject)currentJsonObject,
                            ((FieldAccessor) expression).toString(), true));
                    else
                        jsonObject.put(alias, Operator.pathNavigation(((JSONArray) currentJsonObject).getJSONObject(0),
                                ((FieldAccessor) expression).toString(), true));

                } else if (expression.getKind() == Expression.Kind.CALL_EXPRESSION) {
                    CallExpr callExpression = ((CallExpr) expression);
                    String funcName = callExpression.getFunctionName().toLowerCase();
                    String field = callExpression.getExprList().get(0).toString();

                    Object result = null;
                    if (currentJsonObject instanceof JSONObject) {
                        result = ExpressionEvaluator.evaluateAggregateExpression(
                                field, funcName, collection);
                    } else {
                        result = ExpressionEvaluator.evaluateAggregateExpression(
                                field, funcName, collection.getJSONArray(i));
                    }

                    if (funcName.equalsIgnoreCase("count"))
                        jsonObject.put(String.format("%s(*)", funcName), result);
                    else
                        jsonObject.put(String.format("%s(%s)", funcName, field), result);
                }
            }
            finalCollection.put(jsonObject);
        }
        return finalCollection;
    }

    public JSONArray handleFromClause(FromClause fromClause, JSONObject outerScope) throws GSException, JSONException {
        List<FromTerm> fromTerms = fromClause.getFromTerms();
        List<JSONArray> collections = new ArrayList<>();
        List<String> aliases = new ArrayList<>();
        boolean flag = false;

        for (FromTerm fromTerm : fromTerms) {
            String alias = null;
            Expression expression = fromTerm.getLeftExpression();
            if (expression.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
                if (fromTerm.getLeftVariable() != null)
                    alias = fromTerm.getLeftVariable().getVar().getValue();
                else {
                    alias = ((VariableExpr) expression).getVar().getValue();
                    flag = true;
                }
                collections.add(fetchCollection(((VariableExpr) expression).getVar().getValue()));
            } else if (expression.getKind() == Expression.Kind.SELECT_EXPRESSION) {
                alias = fromTerm.getLeftVariable().getVar().getValue();
                collections.add(fetchCollection(Helper.getSelectBlock((SelectExpression) expression), outerScope));
            } else if (expression.getKind() == Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
                alias = fromTerm.getLeftVariable().getVar().getValue();
                collections.add((JSONArray)Operator.pathNavigation(outerScope,
                        ((FieldAccessor) expression).toString(), true));
            }
            aliases.add(alias);
        }

        //  if (collections.size() == 1)
        //    return collections.get(0);

        JSONArray finalCollection = new JSONArray();


        for (int k=0; k<collections.get(0).length(); k++) {
            JSONObject innerJsonObject = collections.get(0).getJSONObject(k);
            JSONObject finalJsonObject = new JSONObject();
            Iterator<String> keys = innerJsonObject.keys();
            while( keys.hasNext() ) {
                String key = keys.next();
                if (collections.size() > 1 || !flag)
                    finalJsonObject.put( aliases.get(0)+"."+key, innerJsonObject.get(key));
                else
                    finalJsonObject.put( key, innerJsonObject.get(key));
            }
            finalCollection.put(finalJsonObject);
        }

        for (int i=1; i<collections.size(); i++) {
            JSONArray currentCollection =  collections.get(i);
            String alias = aliases.get(i);
            finalCollection = Join.loopJoin(finalCollection, currentCollection, alias);
        }

        return finalCollection;
    }

    public JSONArray handleWhereClause(WhereClause whereClause, JSONArray collection, JSONObject outerScope) throws JSONException {
        JSONArray finalCollection = new JSONArray();

        if (whereClause == null)
            return collection;

        OperatorExpr operatorExpr = (OperatorExpr)whereClause.getWhereExpr();
        List<Expression> expressionList = operatorExpr.getExprList();
        List<OperatorType> operatorTypes = operatorExpr.getOpList();
        for (int i=0; i< collection.length(); i++) {
            JSONObject row = new JSONObject(collection.getJSONObject(i).toString());
            if (outerScope != null)
                row = Helper.appendAttributes(row, outerScope);
            if (ExpressionEvaluator.evaluateExpression(expressionList, operatorTypes, row))
                finalCollection.put(collection.get(i));
        }
        return finalCollection;
    }

    public JSONArray fetchCollection(String name) throws GSException, JSONException {
        if (namedCollections.containsKey(name))
            return namedCollections.get(name);

        if (name.equals(OBSERVATION_COLLECTION))
            return fetchAllObservation();

        if (name.equals(SO_COLLECTION))
            return fetchAllSemanticObservation();

        return gridDBManager.getContainerData(name);
    }

    public JSONArray fetchSemanticObservationFromType(JSONArray typeCollection) throws GSException, JSONException {
        JSONArray finalCollection = new JSONArray();
        for (int i=0; i<typeCollection.length(); i++) {
            JSONArray currentCollection;
            String typeId = null;
            JSONObject type = null;
            try {
                typeId = typeCollection.getJSONObject(i).getString("SemanticObservationType.id");
                type = Helper.stripPath(typeCollection.getJSONObject(i));
                currentCollection = fetchSemanticObservationCollection(typeId);
            } catch (Exception e) {
                typeId = typeCollection.getJSONObject(i).getString("id");
                type = typeCollection.getJSONObject(i);
                currentCollection = fetchSemanticObservationCollection(typeId);
            }
            for (int j=0; j<currentCollection.length(); j++) {
                finalCollection.put(
                        Helper.handlePayloadInSemanticObservation(
                                currentCollection.getJSONObject(j).put("type", type)));
            }
        }
        return finalCollection;
    }

    public JSONArray fetchObservationFromSensor(JSONArray sensorCollection) throws GSException, JSONException {
        JSONArray finalCollection = new JSONArray();
        for (int i=0; i<sensorCollection.length(); i++) {
            JSONArray currentCollection;
            String sensorID = null;
            JSONObject sensor = null;
            try {
                sensorID = sensorCollection.getJSONObject(i).getString("Sensor.id");
                sensor = Helper.stripPath(sensorCollection.getJSONObject(i));
                currentCollection = fetchObservationCollection(sensorID);
            } catch (Exception e) {
                sensorID = sensorCollection.getJSONObject(i).getString("id");
                sensor = sensorCollection.getJSONObject(i);
                currentCollection = fetchObservationCollection(sensorID);
            }
            for (int j=0; j<currentCollection.length(); j++) {
                finalCollection.put(
                        Helper.handlePayloadInObservation(
                                currentCollection.getJSONObject(j).put("sensor", sensor)));
            }
        }
        return finalCollection;
    }

    public JSONArray handleLimitClause(LimitClause limitClause, JSONArray collection) throws JSONException {
        JSONArray finalCollection = new JSONArray();
        if (limitClause == null)
            return collection;

        int limitValue=((Long)((LiteralExpr)limitClause.getLimitExpr()).getValue().getValue()).intValue();
        if(limitValue > collection.length())
            return collection;

        for (int i=0; i<limitValue; i++) {
            finalCollection.put(collection.get(i));
        }

        return finalCollection;
    }

    public JSONArray handleGroupByClause(GroupbyClause groupbyClause, JSONArray collection) throws JSONException {
        if (groupbyClause == null)
            return collection;

        GroupBy groupBy = new GroupBy();
        return groupBy.doGroupBy(collection, groupbyClause);
    }

    public JSONArray fetchObservationCollection(String name) throws GSException, JSONException {
        return fetchCollection(String.format(GridDBCollections.OBSERVATION_COLLECTION, name));
    }

    public JSONArray fetchSemanticObservationCollection(String name) throws GSException, JSONException {
        return fetchCollection(String.format(GridDBCollections.SO_COLLECTION, name));
    }

    public JSONArray fetchAllObservation() throws GSException, JSONException {
        return fetchObservationFromSensor(fetchCollection(SENSOR_COLLECTION));
    }

    public JSONArray fetchAllSemanticObservation() throws GSException, JSONException {
        return fetchSemanticObservationFromType(fetchCollection(SO_TYPE_COLLECTION));
    }

    public JSONArray fetchCollection(SelectBlock selectBlock, JSONObject outerScope) throws GSException, JSONException {
        JSONArray collection = handleFromClause(selectBlock.getFromClause(), outerScope);
        collection = handleWhereClause(selectBlock.getWhereClause(), collection, outerScope);
        collection = handleGroupByClause(selectBlock.getGroupbyClause(), collection);
        collection = handleSelectClause(selectBlock.getSelectClause(), collection, outerScope);

        if (query!= null)
            collection = handleLimitClause(Helper.getLimitClause(query),collection);
        return collection;
    }

    public JSONArray getData() throws GSException, JSONException {
        return fetchCollection(Helper.getSelectBlock(query), null);
    }

}
