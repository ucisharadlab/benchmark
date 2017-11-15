package edu.uci.ics.tippers.common.functions;

import java.io.Serializable;

public class FunctionIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String name;
    private final int arity;

    public static final int VARARGS = -1;

    public FunctionIdentifier(String namespace, String name) {
        this(namespace, name, VARARGS);
    }

    public FunctionIdentifier(String namespace, String name, int arity) {
        this.namespace = namespace;
        this.name = name;
        this.arity = arity;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            return true;
        }
        if (o instanceof FunctionIdentifier) {
            FunctionIdentifier ofi = (FunctionIdentifier) o;
            return ofi.getNamespace().equals(getNamespace()) && ofi.name.equals(name);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return name.hashCode() + namespace.hashCode();
    }

    @Override
    public String toString() {
        return getNamespace() + ":" + name;
    }

    public int getArity() {
        return arity;
    }

    public String getNamespace() {
        return namespace;
    }
}
