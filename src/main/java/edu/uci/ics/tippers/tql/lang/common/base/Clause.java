/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.base;

public interface Clause {
    public ClauseType getClauseType();

    public enum ClauseType {
        FOR_CLAUSE,
        LET_CLAUSE,
        WHERE_CLAUSE,
        GROUP_BY_CLAUSE,
        DISTINCT_BY_CLAUSE,
        ORDER_BY_CLAUSE,
        LIMIT_CLAUSE,
        UPDATE_CLAUSE,

        // SQL related clause
        FROM_CLAUSE,
        FROM_TERM,
        HAVING_CLAUSE,
        JOIN_CLAUSE,
        NEST_CLAUSE,
        PROJECTION,
        SELECT_BLOCK,
        SELECT_CLAUSE,
        SELECT_ELEMENT,
        SELECT_REGULAR,
        SELECT_SET_OPERATION,
        UNNEST_CLAUSE
    }

}
