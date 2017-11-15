package edu.uci.ics.tippers.common.functions;

import java.io.Serializable;

public class FunctionSignature implements Serializable {
    private static final long serialVersionUID = 1L;
    private String namespace;
    private String name;
    private int arity;

    public FunctionSignature(String namespace, String name, int arity) {
        this.namespace = namespace;
        this.name = name;
        this.arity = arity;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FunctionSignature)) {
            return false;
        } else {
            FunctionSignature f = ((FunctionSignature) o);
            return ((namespace != null && namespace.equals(f.getNamespace()) || (namespace == null && f.getNamespace() == null)))
                    && name.equals(f.getName())
                    && (arity == f.getArity() || arity == FunctionIdentifier.VARARGS || f.getArity() == FunctionIdentifier.VARARGS);
        }
    }

    @Override
    public String toString() {
        return namespace + "." + name + "@" + arity;
    }

    @Override
    public int hashCode() {
        return (namespace + "." + name).hashCode();
    }

    public String getNamespace() {
        return namespace;
    }

    public String getName() {
        return name;
    }

    public int getArity() {
        return arity;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setArity(int arity) {
        this.arity = arity;
    }

}

