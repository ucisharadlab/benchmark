/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.lang.sqlpp.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import org.apache.commons.lang3.ObjectUtils;

public class Projection implements Clause {

    private Expression expr;
    private String name;

    public Projection() {

    }

    public Projection(Expression expr, String name) {
        this.expr = expr;
        this.name = name;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.PROJECTION;
    }

    public Expression getExpression() {
        return expr;
    }

    public void setExpression(Expression expr) {
        this.expr = expr;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean hasName() {
        return name != null;
    }

    @Override
    public String toString() {
        return (String.valueOf(expr) +  (hasName() ? " as " + getName() : ""));
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(expr, name);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Projection)) {
            return false;
        }
        Projection target = (Projection) object;
        return ObjectUtils.equals(expr, target.expr)
                && ObjectUtils.equals(name, target.name);
    }
}
