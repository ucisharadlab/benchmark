/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;

public class WhereClause implements Clause {
    private Expression whereExpr;

    public WhereClause() {
        // Default constructor.
    }

    public WhereClause(Expression whereExpr) {
        super();
        this.whereExpr = whereExpr;
    }

    public Expression getWhereExpr() {
        return whereExpr;
    }

    public void setWhereExpr(Expression whereExpr) {
        this.whereExpr = whereExpr;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.WHERE_CLAUSE;
    }


    @Override
    public int hashCode() {
        return whereExpr.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof WhereClause)) {
            return false;
        }
        WhereClause whereClause = (WhereClause) object;
        return whereExpr.equals(whereClause.getWhereExpr());
    }
}
