/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.lang.sqlpp.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;
import edu.uci.ics.tippers.tql.lang.common.clause.GroupbyClause;
import edu.uci.ics.tippers.tql.lang.common.clause.WhereClause;
import org.apache.commons.lang3.ObjectUtils;

public class SelectBlock implements Clause {

    private SelectClause selectClause;
    private FromClause fromClause;
    private WhereClause whereClause;
    private GroupbyClause groupbyClause;


    public SelectBlock(SelectClause selectClause, FromClause fromClause,
            WhereClause whereClause, GroupbyClause groupbyClause) {
        this.selectClause = selectClause;
        this.fromClause = fromClause;

        this.whereClause = whereClause;
        this.groupbyClause = groupbyClause;
    }

    public SelectBlock(SelectClause selectClause, FromClause fromClause,
                       WhereClause whereClause) {
        this.selectClause = selectClause;
        this.fromClause = fromClause;

        this.whereClause = whereClause;
    }

    public GroupbyClause getGroupbyClause() {
        return groupbyClause;
    }

    public void setGroupbyClause(GroupbyClause groupbyClause) {
        this.groupbyClause = groupbyClause;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.SELECT_BLOCK;
    }

    public SelectClause getSelectClause() {
        return selectClause;
    }

    public FromClause getFromClause() {
        return fromClause;
    }

    public WhereClause getWhereClause() {
        return whereClause;
    }

    public boolean hasFromClause() {
        return fromClause != null;
    }

    public boolean hasWhereClause() {
        return whereClause != null;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(fromClause,
                selectClause, whereClause);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SelectBlock)) {
            return false;
        }
        SelectBlock target = (SelectBlock) object;
        boolean equals = ObjectUtils.equals(fromClause, target.fromClause);

        return equals
                && ObjectUtils.equals(selectClause, target.selectClause)
                && ObjectUtils.equals(whereClause, target.whereClause);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(selectClause);
        if (hasFromClause()) {
            sb.append(fromClause);
        }

        if (hasWhereClause()) {
            sb.append(whereClause);
        }
        return sb.toString();
    }
}
