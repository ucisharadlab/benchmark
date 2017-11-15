/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.expression;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import org.apache.commons.lang3.ObjectUtils;

import java.util.List;

public class CallExpr implements Expression {
    private List<Expression> exprList;
    private boolean isBuiltin;
    private String functionName;

    public CallExpr(String functionName, List<Expression> exprList) {
        this.functionName = functionName;
        this.exprList = exprList;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public List<Expression> getExprList() {
        return exprList;
    }

    public boolean isBuiltin() {
        return isBuiltin;
    }

    @Override
    public Kind getKind() {
        return Kind.CALL_EXPRESSION;
    }

    public void setExprList(List<Expression> exprList) {
        this.exprList = exprList;
    }

    @Override
    public String toString() {
        return "call " + functionName;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(exprList, functionName, isBuiltin);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof CallExpr)) {
            return false;
        }
        CallExpr target = (CallExpr) object;
        return ObjectUtils.equals(exprList, target.exprList)
                && ObjectUtils.equals(functionName, target.functionName) && isBuiltin == target.isBuiltin;
    }
}
