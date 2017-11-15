/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.literal;

import edu.uci.ics.tippers.tql.lang.common.base.Literal;

public class MissingLiteral extends Literal {
    private static final long serialVersionUID = 1L;
    public static final MissingLiteral INSTANCE = new MissingLiteral();

    private MissingLiteral() {
    }

    @Override
    public Type getLiteralType() {
        return Type.MISSING;
    }

    @Override
    public String getStringValue() {
        return "missing";
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
