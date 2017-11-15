/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.lang.sqlpp.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;
import org.apache.commons.lang3.ObjectUtils;

public class SelectClause implements Clause {

    private SelectRegular selectRegular;

    public SelectClause(SelectRegular selectRegular) {
        this.selectRegular = selectRegular;
    }


    @Override
    public ClauseType getClauseType() {
        return ClauseType.SELECT_CLAUSE;
    }

    public SelectRegular getSelectRegular() {
        return selectRegular;
    }

    public boolean selectRegular() {
        return selectRegular != null;
    }

    @Override
    public String toString() {
        return "select "
                + String.valueOf(selectRegular);
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(selectRegular);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SelectClause)) {
            return false;
        }
        SelectClause target = (SelectClause) object;
        return ObjectUtils.equals(selectRegular, target.selectRegular);
    }
}
