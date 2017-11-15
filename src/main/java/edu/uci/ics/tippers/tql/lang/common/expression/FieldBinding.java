/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.expression;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import org.apache.commons.lang3.ObjectUtils;

public class FieldBinding {
    private Expression leftExpr;
    private Expression rightExpr;

    public FieldBinding() {
        // default constructor.
    }

    public FieldBinding(Expression leftExpr, Expression rightExpr) {
        super();
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
    }

    public Expression getLeftExpr() {
        return leftExpr;
    }

    public void setLeftExpr(Expression leftExpr) {
        this.leftExpr = leftExpr;
    }

    public Expression getRightExpr() {
        return rightExpr;
    }

    public void setRightExpr(Expression rightExpr) {
        this.rightExpr = rightExpr;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(leftExpr, rightExpr);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FieldBinding)) {
            return false;
        }
        FieldBinding target = (FieldBinding) object;
        return ObjectUtils.equals(leftExpr, target.leftExpr) && ObjectUtils.equals(rightExpr, target.rightExpr);
    }

    @Override
    public String toString() {
        return leftExpr + ": " + rightExpr;
    }

}
