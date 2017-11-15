/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.lang.sqlpp.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.expression.VariableExpr;
import edu.uci.ics.tippers.tql.lang.sqlpp.optype.JoinType;

public class JoinClause {

    public JoinClause(JoinType joinType, Expression rightExpr, VariableExpr rightVar, VariableExpr rightPosVar,
            Expression conditionExpr) {
    }

    public Clause.ClauseType getClauseType() {
        return Clause.ClauseType.JOIN_CLAUSE;
    }

}
