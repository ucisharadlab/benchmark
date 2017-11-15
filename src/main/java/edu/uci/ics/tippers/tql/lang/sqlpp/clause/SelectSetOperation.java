/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.lang.sqlpp.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;
import edu.uci.ics.tippers.tql.lang.sqlpp.struct.SetOperationInput;
import org.apache.commons.lang3.ObjectUtils;

public class SelectSetOperation implements Clause {

    private SetOperationInput leftInput;

    public SelectSetOperation(SetOperationInput leftInput) {
        this.leftInput = leftInput;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.SELECT_SET_OPERATION;
    }

    public SetOperationInput getLeftInput() {
        return leftInput;
    }


    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(leftInput);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SelectSetOperation)) {
            return false;
        }
        SelectSetOperation target = (SelectSetOperation) object;
        return ObjectUtils.equals(leftInput, target.leftInput);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(leftInput);
        return sb.toString();
    }

}
