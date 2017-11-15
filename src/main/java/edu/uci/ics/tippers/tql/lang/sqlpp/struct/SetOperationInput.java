/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.sqlpp.struct;

import edu.uci.ics.tippers.tql.lang.sqlpp.clause.SelectBlock;
import org.apache.commons.lang3.ObjectUtils;

public class SetOperationInput {

    private SelectBlock selectBlock;

    public SetOperationInput(SelectBlock selectBlock) {
        this.selectBlock = selectBlock;
    }

    public SelectBlock getSelectBlock() {
        return selectBlock;
    }

    public boolean selectBlock() {
        return selectBlock != null;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(selectBlock);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SetOperationInput)) {
            return false;
        }
        SetOperationInput target = (SetOperationInput) object;
        return ObjectUtils.equals(selectBlock, target.selectBlock);
    }

    @Override
    public String toString() {
        return selectBlock.toString();
    }
}
