/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.expression;

import edu.uci.ics.tippers.common.exceptions.CompilationException;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.struct.UnaryExprType;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Optional;

public class UnaryExpr implements Expression {
    private UnaryExprType unaryExprType;
    private Expression expr;

    public UnaryExpr() {
        // default constructor
    }

    public UnaryExpr(UnaryExprType type, Expression expr) {
        this.unaryExprType = type;
        this.expr = expr;
    }

    public UnaryExprType getExprType() {
        return unaryExprType;
    }

    public void setExprType(String strType) throws CompilationException {
        Optional<UnaryExprType> exprType = UnaryExprType.fromSymbol(strType);
        if (exprType.isPresent()) {
            this.unaryExprType = exprType.get();
        } else {
            throw new CompilationException("Unsupported operator: " + strType);
        }
    }

    public Expression getExpr() {
        return expr;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

    @Override
    public Kind getKind() {
        return Kind.UNARY_EXPRESSION;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(expr, unaryExprType);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof UnaryExpr)) {
            return false;
        }
        UnaryExpr target = (UnaryExpr) object;
        return ObjectUtils.equals(expr, target.expr) && ObjectUtils.equals(unaryExprType, target.unaryExprType);
    }
}
