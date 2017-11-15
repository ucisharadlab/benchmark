/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.literal;

import edu.uci.ics.tippers.tql.lang.common.base.Literal;
import org.apache.commons.lang3.ObjectUtils;

public class StringLiteral extends Literal {

    private static final long serialVersionUID = -6342491706277606168L;
    private String value;

    public StringLiteral(String value) {
        super();
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public Type getLiteralType() {
        return Type.STRING;
    }

    @Override
    public String getStringValue() {
        return value;
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
        if (!(object instanceof StringLiteral)) {
            return false;
        }
        StringLiteral target = (StringLiteral) object;
        return ObjectUtils.equals(value, target.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
