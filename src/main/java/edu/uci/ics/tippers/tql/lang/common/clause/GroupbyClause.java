/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;
import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import org.apache.commons.lang3.ObjectUtils;

import java.util.List;

public class GroupbyClause implements Clause {

    private List<Expression> expressions;

    public GroupbyClause() {
        // Default constructor.
    }

    public GroupbyClause(List<Expression> expressions) {
        this.expressions = expressions;
    }


    @Override
    public ClauseType getClauseType() {
        return ClauseType.GROUP_BY_CLAUSE;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public void setExpressions(List<Expression> expressions) {
        this.expressions = expressions;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GroupbyClause)) {
            return false;
        }
        GroupbyClause target = (GroupbyClause) object;
        return ObjectUtils.equals(expressions, target.expressions);

    }
}
