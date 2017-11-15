/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.sqlpp.expression;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.clause.LimitClause;
import edu.uci.ics.tippers.tql.lang.sqlpp.clause.SelectSetOperation;
import org.apache.commons.lang3.ObjectUtils;

public class SelectExpression implements Expression {


    private SelectSetOperation selectSetOperation;

    private LimitClause limitClause;
    private boolean subquery;

    public SelectExpression( SelectSetOperation selectSetOperation,
            LimitClause limitClause, boolean subquery) {

        this.selectSetOperation = selectSetOperation;
        this.limitClause = limitClause;
        this.subquery = subquery;
    }

    @Override
    public Kind getKind() {
        return Kind.SELECT_EXPRESSION;
    }


    public SelectSetOperation getSelectSetOperation() {
        return selectSetOperation;
    }


    public LimitClause getLimitClause() {
        return limitClause;
    }


    public boolean hasLimit() {
        return limitClause != null;
    }


    public boolean isSubquery() {
        return subquery;
    }

    public void setSubquery(boolean setSubquery) {
        subquery = setSubquery;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(limitClause, selectSetOperation, subquery);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SelectExpression)) {
            return false;
        }
        SelectExpression target = (SelectExpression) object;
        boolean equals = ObjectUtils.equals(selectSetOperation, target.selectSetOperation);
        return equals && subquery == target.subquery;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(subquery ? "(" : "");
        sb.append(selectSetOperation);
        if (hasLimit()) {
            sb.append(limitClause);
        }
        sb.append(subquery ? ")" : "");
        return sb.toString();
    }
}
