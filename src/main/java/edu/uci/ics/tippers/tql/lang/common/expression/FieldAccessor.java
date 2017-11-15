/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.expression;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.struct.Identifier;
import org.apache.commons.lang3.ObjectUtils;

public class FieldAccessor extends AbstractAccessor {
    private Identifier ident;

    public FieldAccessor(Expression expr, Identifier ident) {
        super(expr);
        this.ident = ident;
    }

    public Identifier getIdent() {
        return ident;
    }

    public void setIdent(Identifier ident) {
        this.ident = ident;
    }

    @Override
    public Kind getKind() {
        return Kind.FIELD_ACCESSOR_EXPRESSION;
    }

    @Override
    public String toString() {
        return String.valueOf(expr) + "." + ident.toString();
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + ObjectUtils.hashCode(ident);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FieldAccessor)) {
            return false;
        }
        FieldAccessor target = (FieldAccessor) object;
        return super.equals(target) && ObjectUtils.equals(ident, target.ident);
    }
}
