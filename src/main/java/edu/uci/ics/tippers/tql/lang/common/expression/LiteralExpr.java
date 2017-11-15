/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.expression;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.base.Literal;
import org.apache.commons.lang3.ObjectUtils;

public class LiteralExpr implements Expression {
    private Literal value;

    public LiteralExpr() {
        // default constructor.
    }

    public LiteralExpr(Literal value) {
        this.value = value;
    }

    public Literal getValue() {
        return value;
    }

    public void setValue(Literal value) {
        this.value = value;
    }

    @Override
    public Kind getKind() {
        return Kind.LITERAL_EXPRESSION;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(value);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LiteralExpr)) {
            return false;
        }
        LiteralExpr target = (LiteralExpr) object;
        return ObjectUtils.equals(value, target.value);
    }

}
