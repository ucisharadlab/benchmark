/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.lang.sqlpp.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;

public class SelectElement implements Clause {

    private Expression expr;

    public SelectElement(Expression expr) {
        this.expr = expr;
    }


    @Override
    public ClauseType getClauseType() {
        return ClauseType.SELECT_ELEMENT;
    }

    public Expression getExpression() {
        return expr;
    }

    public void setExpression(Expression expr) {
        this.expr = expr;
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
        if (!(object instanceof SelectElement)) {
            return false;
        }
        SelectElement target = (SelectElement) object;
        return expr.equals(target.expr);
    }
}
