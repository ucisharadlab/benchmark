package edu.uci.ics.tippers.wrapper.griddb.translator;

import com.toshiba.mwcloud.gs.GSException;
import edu.uci.ics.tippers.wrapper.griddb.util.Helper;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.expression.FieldAccessor;
import edu.uci.ics.tippers.tql.lang.common.expression.LiteralExpr;
import edu.uci.ics.tippers.tql.lang.common.expression.OperatorExpr;
import edu.uci.ics.tippers.tql.lang.common.expression.VariableExpr;
import edu.uci.ics.tippers.tql.lang.common.struct.OperatorType;
import edu.uci.ics.tippers.tql.lang.sqlpp.expression.SelectExpression;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by peeyush on 7/5/17.
 */
public class ExpressionEvaluator {


    public static Boolean evaluateExpression(List<Expression> expressionList, List<OperatorType> operators,
                                              Object row) throws JSONException {
        try {
            return (Boolean) evaluateObjectExpression(expressionList, operators, row);
        } catch (Exception e) {
            return false;
        }
    }

    public static Object evaluateObjectExpression(List<Expression> expressionList, List<OperatorType> operators,
                                             Object row) throws JSONException {

        List<Object> evaluatedExpressions = new ArrayList<>();
        for (Expression expression: expressionList) {
            if (expression instanceof OperatorExpr) {
                evaluatedExpressions.add(evaluateObjectExpression(((OperatorExpr) expression).getExprList(),
                                ((OperatorExpr) expression).getOpList(), row));
            } else if (expression instanceof VariableExpr) {
                evaluatedExpressions.add(Operator.pathNavigation(
                        (JSONObject)row,
                        ((VariableExpr)expression).toString(),
                        false));
            } else if (expression instanceof FieldAccessor) {
                evaluatedExpressions.add(Operator.pathNavigation(
                        (JSONObject)row,
                        ((FieldAccessor)expression).toString(),
                        true));
            } else if (expression instanceof LiteralExpr) {
                evaluatedExpressions.add(((LiteralExpr)expression).getValue().getValue());
            } else if (expression.getKind() == Expression.Kind.SELECT_EXPRESSION) {
                Translator tr = new Translator();
                try {
                    evaluatedExpressions.add(tr.fetchCollection(Helper.getSelectBlock((SelectExpression) expression),
                            (JSONObject) row));
                } catch (GSException e) {
                    e.printStackTrace();
                }
            }
        }

        return evaluateLiteralExpression(evaluatedExpressions, operators);
    }

    public static Object evaluateAggregateExpression(String field, String functionName, JSONArray collection) throws JSONException {
        Optional<OperatorType> function = OperatorType.fromSymbol(functionName);
        switch (function.get()) {
            case AVG:
                return AggregateOperator.avg(field, collection);
            case MIN:
                return AggregateOperator.min(field, collection);
            case MAX:
                return AggregateOperator.max(field, collection);
            case SUM:
                return AggregateOperator.sum(field, collection);
            case COUNT:
                return AggregateOperator.count(collection);
        }
        return null;
    }

    public static Object evaluateLiteralExpression(List<Object> literals, List<OperatorType> operators) throws JSONException {
        if (literals.size() == 1)
            return literals.get(0);

        Object currentAns = literals.get(0);
        for (int i=0; i<literals.size()-1; i++) {
            OperatorType operator = operators.get(i);
            Object leftOperand = currentAns;
            Object rightOperand = literals.get(i+1);

            switch (operator) {
                case AND:
                    currentAns = Operator.and(leftOperand, rightOperand);
                    break;
                case OR:
                    currentAns = Operator.or(leftOperand, rightOperand);
                    break;
                case GE:
                    currentAns = Operator.ge(leftOperand, rightOperand);
                    break;
                case GT:
                    currentAns = Operator.gt(leftOperand, rightOperand);
                    break;
                case LE:
                    currentAns = Operator.le(leftOperand, rightOperand);
                    break;
                case LT:
                    currentAns = Operator.lt(leftOperand, rightOperand);
                    break;
                case EQ:
                    currentAns = Operator.eq(leftOperand, rightOperand);
                    break;
                case NEQ:
                    currentAns = Operator.ne(leftOperand, rightOperand);
                    break;
                case PLUS:
                    currentAns = Operator.plus(leftOperand, rightOperand);
                    break;
                case MINUS:
                    currentAns = Operator.minus(leftOperand, rightOperand);
                    break;
                case MUL:
                    currentAns = Operator.multiply(leftOperand, rightOperand);
                    break;
                case DIV:
                    currentAns = Operator.divide(leftOperand, rightOperand);
                    break;
                case MOD:
                    currentAns = Operator.mod(leftOperand, rightOperand);
                    break;
                case LIKE:
                    currentAns = Operator.like(leftOperand, rightOperand);
                    break;
                case IN:
                    currentAns = Operator.in(leftOperand,(JSONArray) rightOperand);
            }
        }
        return currentAns;
    }
}
