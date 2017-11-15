/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.literal;

import edu.uci.ics.tippers.tql.lang.common.base.Literal;

public class TrueLiteral extends Literal {
    private static final long serialVersionUID = -8513245514578847512L;
    public static final TrueLiteral INSTANCE = new TrueLiteral();

    private TrueLiteral() {
    }

    @Override
    public Type getLiteralType() {
        return Type.TRUE;
    }

    @Override
    public String getStringValue() {
        return "true";
    }

    @Override
    public String toString() {
        return getStringValue();
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
    public Boolean getValue() {
        return Boolean.TRUE;
    }
}
