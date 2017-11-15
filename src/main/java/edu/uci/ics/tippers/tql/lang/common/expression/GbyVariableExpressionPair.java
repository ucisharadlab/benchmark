/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.expression;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import org.apache.commons.lang3.ObjectUtils;

public class GbyVariableExpressionPair {
    private VariableExpr var; // can be null
    private Expression expr;

    public GbyVariableExpressionPair() {
        super();
    }

    public GbyVariableExpressionPair(VariableExpr var, Expression expr) {
        super();
        this.var = var;
        this.expr = expr;
    }

    public VariableExpr getVar() {
        return var;
    }

    public void setVar(VariableExpr var) {
        this.var = var;
    }

    public Expression getExpr() {
        return expr;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(expr, var);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GbyVariableExpressionPair)) {
            return false;
        }
        GbyVariableExpressionPair target = (GbyVariableExpressionPair) object;
        return ObjectUtils.equals(expr, target.expr) && ObjectUtils.equals(var, target.var);
    }

    @Override
    public String toString() {
        return expr + " AS " + var;
    }
}
