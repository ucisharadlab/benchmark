/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.struct;

import org.apache.commons.lang3.ObjectUtils;

public class Identifier {
    protected String value;

    public Identifier() {
        // default constructor.
    }

    public Identifier(String value) {
        this.value = value;
    }

    public final String getValue() {
        return value;
    }

    public final void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Identifier)) {
            return false;
        }
        Identifier target = (Identifier) o;
        return ObjectUtils.equals(value, target.value);
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(value);
    }
}
