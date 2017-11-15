/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.literal;

import edu.uci.ics.tippers.tql.lang.common.base.Literal;
import org.apache.commons.lang3.ObjectUtils;

public class LongIntegerLiteral extends Literal {
    private static final long serialVersionUID = -8633520244871361967L;
    private Long value;

    public LongIntegerLiteral(Long value) {
        super();
        this.value = value;
    }

    @Override
    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    @Override
    public Type getLiteralType() {
        return Type.LONG;
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
        if (!(object instanceof LongIntegerLiteral)) {
            return false;
        }
        LongIntegerLiteral target = (LongIntegerLiteral) object;
        return value.equals(target.getValue());
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
