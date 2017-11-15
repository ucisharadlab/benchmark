/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.literal;

import edu.uci.ics.tippers.tql.lang.common.base.Literal;
import org.apache.commons.lang3.ObjectUtils;

public class IntegerLiteral extends Literal {
    private static final long serialVersionUID = -8633520244871361967L;
    private Integer value;

    public IntegerLiteral(Integer value) {
        super();
        this.value = value;
    }

    @Override
    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public Type getLiteralType() {
        return Type.INTEGER;
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
        if (!(object instanceof IntegerLiteral)) {
            return false;
        }
        IntegerLiteral target = (IntegerLiteral) object;
        return value.equals(target.getValue());
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
