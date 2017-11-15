/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.expression;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import org.apache.commons.lang3.ObjectUtils;

import java.util.List;

public class ListConstructor implements Expression {
    private List<Expression> exprList;
    private Type type;

    public ListConstructor() {
        // default constructor.
    }

    public ListConstructor(Type type, List<Expression> exprList) {
        this.type = type;
        this.exprList = exprList;
    }

    public List<Expression> getExprList() {
        return exprList;
    }

    public void setExprList(List<Expression> exprList) {
        this.exprList = exprList;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public Kind getKind() {
        return Kind.LIST_CONSTRUCTOR_EXPRESSION;
    }

    public enum Type {
        ORDERED_LIST_CONSTRUCTOR,
        UNORDERED_LIST_CONSTRUCTOR
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(exprList, type);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ListConstructor)) {
            return false;
        }
        ListConstructor target = (ListConstructor) object;
        return ObjectUtils.equals(exprList, target.exprList) && ObjectUtils.equals(type, target.type);
    }
}
