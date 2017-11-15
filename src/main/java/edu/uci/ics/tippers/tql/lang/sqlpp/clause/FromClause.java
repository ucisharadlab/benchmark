/*

 Taken From AsterixDB Source Code on GitHub
 */

package edu.uci.ics.tippers.tql.lang.sqlpp.clause;

import edu.uci.ics.tippers.tql.lang.common.base.Clause;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FromClause implements Clause {

    private List<FromTerm> fromTerms = new ArrayList<>();

    public FromClause(List<FromTerm> fromTerms) {
        this.fromTerms = fromTerms;
    }


    @Override
    public ClauseType getClauseType() {
        return ClauseType.FROM_CLAUSE;
    }

    public List<FromTerm> getFromTerms() {
        return fromTerms;
    }

    @Override
    public String toString() {
        return fromTerms.stream().map(String::valueOf).collect(Collectors.joining(", "));
    }

    @Override
    public int hashCode() {
        return fromTerms.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FromClause)) {
            return false;
        }
        FromClause target = (FromClause) object;
        return fromTerms.equals(target.getFromTerms());
    }
}
