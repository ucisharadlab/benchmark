/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.lang.sqlpp.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;

import java.util.List;
import java.util.stream.Collectors;

public class SelectRegular implements Clause {

    private List<Projection> projections;

    public SelectRegular(List<Projection> projections) {
        this.projections = projections;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.SELECT_REGULAR;
    }

    public List<Projection> getProjections() {
        return projections;
    }

    @Override
    public String toString() {
        return projections.stream().map(String::valueOf).collect(Collectors.joining(", "));
    }

    @Override
    public int hashCode() {
        return projections.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SelectRegular)) {
            return false;
        }
        SelectRegular target = (SelectRegular) object;
        return projections.equals(target.getProjections());
    }
}
