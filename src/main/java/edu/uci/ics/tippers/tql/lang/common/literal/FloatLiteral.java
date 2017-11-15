/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.literal;

import edu.uci.ics.tippers.tql.lang.common.base.Literal;
import org.apache.commons.lang3.ObjectUtils;

public class FloatLiteral extends Literal {
    private static final long serialVersionUID = 3273563021227964396L;
    private Float value;

    public FloatLiteral(Float value) {
        super();
        this.value = value;
    }

    @Override
    public Float getValue() {
        return value;
    }

    public void setValue(Float value) {
        this.value = value;
    }

    @Override
    public Type getLiteralType() {
        return Type.FLOAT;
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
        if (!(object instanceof FloatLiteral)) {
            return false;
        }
        FloatLiteral target = (FloatLiteral) object;
        return ObjectUtils.equals(value, target.value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
