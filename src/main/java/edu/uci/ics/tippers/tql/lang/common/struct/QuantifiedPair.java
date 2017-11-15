/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.struct;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.expression.VariableExpr;
import org.apache.commons.lang3.ObjectUtils;

public class QuantifiedPair {
    private VariableExpr varExpr;
    private Expression expr;

    public QuantifiedPair() {
        // default constructor
    }

    public QuantifiedPair(VariableExpr varExpr, Expression expr) {
        this.varExpr = varExpr;
        this.expr = expr;
    }

    public VariableExpr getVarExpr() {
        return varExpr;
    }

    public void setVarExpr(VariableExpr varExpr) {
        this.varExpr = varExpr;
    }

    public Expression getExpr() {
        return expr;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(expr, varExpr);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof QuantifiedPair)) {
            return false;
        }
        QuantifiedPair target = (QuantifiedPair) object;
        return ObjectUtils.equals(expr, target.expr) && ObjectUtils.equals(varExpr, target.varExpr);
    }
}
