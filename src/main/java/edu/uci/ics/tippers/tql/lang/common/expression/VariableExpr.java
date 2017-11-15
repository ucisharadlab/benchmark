/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.expression;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.struct.VarIdentifier;
import org.apache.commons.lang3.ObjectUtils;

public class VariableExpr implements Expression {
    private VarIdentifier var;
    private boolean isNewVar;

    public VariableExpr() {
        super();
        isNewVar = true;
    }

    public VariableExpr(VarIdentifier var) {
        super();
        this.var = var;
        isNewVar = true;
    }

    public boolean getIsNewVar() {
        return isNewVar;
    }

    public void setIsNewVar(boolean isNewVar) {
        this.isNewVar = isNewVar;
    }

    public VarIdentifier getVar() {
        return var;
    }

    public void setVar(VarIdentifier var) {
        this.var = var;
    }

    @Override
    public Kind getKind() {
        return Kind.VARIABLE_EXPRESSION;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(var);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof VariableExpr)) {
            return false;
        }
        VariableExpr expr = (VariableExpr) obj;
        return ObjectUtils.equals(var, expr.var);
    }

    @Override
    public String toString() {
        return var.getValue();
    }
}
