/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.expression;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import org.apache.commons.lang3.ObjectUtils;

public abstract class AbstractAccessor implements Expression {
    protected Expression expr;

    public AbstractAccessor(Expression expr) {
        super();
        this.expr = expr;
    }

    public Expression getExpr() {
        return expr;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(expr);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof AbstractAccessor)) {
            return false;
        }
        AbstractAccessor target = (AbstractAccessor) object;
        return expr.equals(target.expr);
    }

}
