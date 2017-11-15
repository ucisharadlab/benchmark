/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import org.apache.commons.lang3.ObjectUtils;

public class LimitClause implements Clause {
    private Expression limitExpr;
    private Expression offset;

    public LimitClause() {
        // Default constructor.
    }

    public LimitClause(Expression limitexpr, Expression offset) {
        this.limitExpr = limitexpr;
        this.offset = offset;
    }

    public Expression getLimitExpr() {
        return limitExpr;
    }

    public void setLimitExpr(Expression limitexpr) {
        this.limitExpr = limitexpr;
    }

    public Expression getOffset() {
        return offset;
    }

    public void setOffset(Expression offset) {
        this.offset = offset;
    }

    public boolean hasOffset() {
        return offset != null;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.LIMIT_CLAUSE;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(limitExpr, offset);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LimitClause)) {
            return false;
        }
        LimitClause target = (LimitClause) object;
        return limitExpr.equals(target.getLimitExpr()) && ObjectUtils.equals(offset, target.getOffset());
    }
}
