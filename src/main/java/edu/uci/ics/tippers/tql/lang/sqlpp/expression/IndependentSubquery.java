/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.lang.sqlpp.expression;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;


public class IndependentSubquery implements Expression {

    private Expression expr;

    public IndependentSubquery(Expression enclosedExpr) {
        this.expr = enclosedExpr;
    }

    @Override
    public Kind getKind() {
        return Kind.INDEPENDENT_SUBQUERY;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

    public Expression getExpr() {
        return this.expr;
    }

    @Override
    public String toString() {
        return String.valueOf(expr);
    }

    @Override
    public int hashCode() {
        return expr.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof IndependentSubquery)) {
            return false;
        }
        IndependentSubquery target = (IndependentSubquery) object;
        return this.expr.equals(target.getExpr());
    }
}
