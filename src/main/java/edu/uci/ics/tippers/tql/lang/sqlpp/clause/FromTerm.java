/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.lang.sqlpp.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.expression.VariableExpr;
import org.apache.commons.lang3.ObjectUtils;

public class FromTerm implements Clause {
    private Expression leftExpr;
    private VariableExpr leftVar;

    public FromTerm(Expression leftExpr, VariableExpr leftVar) {
        this.leftExpr = leftExpr;
        this.leftVar = leftVar;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.FROM_TERM;
    }

    public Expression getLeftExpression() {
        return leftExpr;
    }

    public void setLeftExpression(Expression expr) {
        this.leftExpr = expr;
    }

    public VariableExpr getLeftVariable() {
        return leftVar;
    }

    @Override
    public String toString() {
        return String.valueOf(leftExpr) + " AS " + leftVar;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FromTerm)) {
            return false;
        }
        FromTerm target = (FromTerm) object;
        return  ObjectUtils.equals(leftExpr, target.leftExpr) && ObjectUtils.equals(leftVar, target.leftVar);
    }
}
