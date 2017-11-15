/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.expression;

import edu.uci.ics.tippers.common.exceptions.CompilationException;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.struct.OperatorType;
import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OperatorExpr implements Expression{
    private List<Expression> exprList;
    private List<OperatorType> opList;
    private List<Integer> exprBroadcastIdx;
    private boolean currentop = false;

    public OperatorExpr() {
        super();
        exprList = new ArrayList<>();
        exprBroadcastIdx = new ArrayList<>();
        opList = new ArrayList<>();
    }

    public OperatorExpr(List<Expression> exprList, List<Integer> exprBroadcastIdx, List<OperatorType> opList,
            boolean currentop) {
        this.exprList = exprList;
        this.exprBroadcastIdx = exprBroadcastIdx;
        this.opList = opList;
        this.currentop = currentop;
    }

    public boolean isCurrentop() {
        return currentop;
    }

    public void setCurrentop(boolean currentop) {
        this.currentop = currentop;
    }

    public List<Expression> getExprList() {
        return exprList;
    }

    public List<Integer> getExprBroadcastIdx() {
        return exprBroadcastIdx;
    }

    public List<OperatorType> getOpList() {
        return opList;
    }

    public void setExprList(List<Expression> exprList) {
        this.exprList = exprList;
    }

    public void addOperand(Expression operand) {
        addOperand(operand, false);
    }

    public void addOperand(Expression operand, boolean broadcast) {
        if (broadcast) {
            exprBroadcastIdx.add(exprList.size());
        }
        exprList.add(operand);
    }

    public static final boolean opIsComparison(OperatorType t) {
        boolean cmp = t == OperatorType.EQ || t == OperatorType.NEQ || t == OperatorType.GT;
        cmp = cmp || t == OperatorType.GE || t == OperatorType.LT || t == OperatorType.LE;
        return cmp;
    }

    public void addOperator(String strOp) throws CompilationException {
        Optional<OperatorType> op = OperatorType.fromSymbol(strOp);
        if (op.isPresent()) {
            opList.add(op.get());
        } else {
            throw new CompilationException("Unsupported operator: " + strOp);
        }
    }

    public Expression.Kind getKind() {
        return Expression.Kind.OP_EXPRESSION;
    }


    public boolean isBroadcastOperand(int idx) {
        for (Integer i : exprBroadcastIdx) {
            if (i == idx) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(currentop, exprBroadcastIdx, exprList, opList);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof OperatorExpr)) {
            return false;
        }
        OperatorExpr target = (OperatorExpr) object;
        return currentop == target.isCurrentop() && ObjectUtils.equals(exprBroadcastIdx, target.exprBroadcastIdx)
                && ObjectUtils.equals(exprList, target.exprList) && ObjectUtils.equals(opList, target.opList);
    }
}
