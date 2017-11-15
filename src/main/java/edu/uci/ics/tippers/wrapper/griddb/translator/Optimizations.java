package edu.uci.ics.tippers.wrapper.griddb.translator;

import com.toshiba.mwcloud.gs.GSException;
import edu.uci.ics.tippers.wrapper.griddb.util.Helper;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.clause.WhereClause;
import edu.uci.ics.tippers.tql.lang.common.expression.FieldAccessor;
import edu.uci.ics.tippers.tql.lang.common.expression.VariableExpr;
import edu.uci.ics.tippers.tql.lang.sqlpp.clause.FromClause;
import edu.uci.ics.tippers.tql.lang.sqlpp.clause.FromTerm;
import edu.uci.ics.tippers.tql.lang.sqlpp.expression.SelectExpression;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by peeyush on 11/8/17.
 */
public class Optimizations {

    private Translator translator;

    public Optimizations(Translator translator) {
        this.translator = translator;
    }

    public JSONArray handleFromClause(FromClause fromClause, WhereClause whereClause, JSONObject outerScope)
            throws JSONException, GSException {

        if (whereClause == null)
            return translator.handleFromClause(fromClause, outerScope);

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
                collections.add(fetchCollection(((VariableExpr) expression).getVar().getValue(), whereClause));
            } else if (expression.getKind() == Expression.Kind.SELECT_EXPRESSION) {
                alias = fromTerm.getLeftVariable().getVar().getValue();
                collections.add(
                        translator.fetchCollection(Helper.getSelectBlock((SelectExpression) expression), outerScope));
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

    public JSONArray fetchCollection(String name, WhereClause whereClause) throws GSException, JSONException {

//        if (name.equals(Translator.OBSERVATION_COLLECTION))
//            return fetchAllObservation();
//
//        if (name.equals(Translator.SO_COLLECTION))
//            return fetchAllSemanticObservation();

        return translator.fetchCollection(name);
    }

}
