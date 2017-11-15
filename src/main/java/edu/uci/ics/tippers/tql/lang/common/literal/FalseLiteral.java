/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.literal;

import edu.uci.ics.tippers.tql.lang.common.base.Literal;

public class FalseLiteral extends Literal {

    private static final long serialVersionUID = -750814844423165149L;
    public static final FalseLiteral INSTANCE = new FalseLiteral();

    private FalseLiteral() {
    }

    @Override
    public Type getLiteralType() {
        return Type.FALSE;
    }

    @Override
    public String getStringValue() {
        return "false";
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public Boolean getValue() {
        return Boolean.FALSE;
    }

    @Override
    public int hashCode() {
        return (int) serialVersionUID;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == INSTANCE;
    }

}
