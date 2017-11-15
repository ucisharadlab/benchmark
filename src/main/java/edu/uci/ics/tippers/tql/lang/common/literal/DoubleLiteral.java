/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.literal;

import edu.uci.ics.tippers.tql.lang.common.base.Literal;
import org.apache.commons.lang3.ObjectUtils;

public class DoubleLiteral extends Literal {
    private static final long serialVersionUID = -5685491458356989250L;
    private Double value;

    public DoubleLiteral(Double value) {
        super();
        this.value = value;
    }

    @Override
    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    @Override
    public Type getLiteralType() {
        return Type.DOUBLE;
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
        if (!(object instanceof DoubleLiteral)) {
            return false;
        }
        DoubleLiteral target = (DoubleLiteral) object;
        return ObjectUtils.equals(value, target.value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
