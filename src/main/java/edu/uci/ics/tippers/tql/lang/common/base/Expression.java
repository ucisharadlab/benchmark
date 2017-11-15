/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.base;

public interface Expression {
    public abstract Kind getKind();

    public enum Kind {
        LITERAL_EXPRESSION,
        LIST_CONSTRUCTOR_EXPRESSION,
        VARIABLE_EXPRESSION,
        OP_EXPRESSION,
        FIELD_ACCESSOR_EXPRESSION,
        UNARY_EXPRESSION,
        SELECT_EXPRESSION,
        INDEPENDENT_SUBQUERY,
        CALL_EXPRESSION
    }

}
