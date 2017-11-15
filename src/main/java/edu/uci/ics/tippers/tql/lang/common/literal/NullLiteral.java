/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.literal;

import edu.uci.ics.tippers.tql.lang.common.base.Literal;

public class NullLiteral extends Literal {
    private static final long serialVersionUID = -7782153599294838739L;
    public static final NullLiteral INSTANCE = new NullLiteral();

    private NullLiteral() {
    }

    @Override
    public Type getLiteralType() {
        return Type.NULL;
    }

    @Override
    public String getStringValue() {
        return "null";
    }

    @Override
    public boolean equals(Object obj) {
        return obj == INSTANCE;
    }

    @Override
    public int hashCode() {
        return (int) serialVersionUID;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public String toString() {
        return getStringValue();
    }
}
