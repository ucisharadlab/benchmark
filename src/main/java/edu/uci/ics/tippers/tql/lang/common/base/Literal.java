/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.base;

import java.io.Serializable;

public abstract class Literal implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = -6468144574890768345L;

    public enum Type {
        STRING,
        INTEGER,
        MISSING,
        NULL,
        TRUE,
        FALSE,
        FLOAT,
        DOUBLE,
        LONG
    }

    abstract public Object getValue();

    abstract public Type getLiteralType();

    public String getStringValue() {
        return getValue().toString();
    }

    @Override
    public int hashCode() {
        return getValue().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Literal)) {
            return false;
        }
        Literal literal = (Literal) obj;
        return getValue().equals(literal.getValue());
    }

    @Override
    public String toString() {
        return getStringValue();
    }
}
