/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.struct;

import java.util.Arrays;
import java.util.Optional;

public enum OperatorType {
    OR("or"),
    AND("and"),
    LT("<"),
    GT(">"),
    LE("<="),
    GE(">="),
    EQ("="),
    NEQ("!="),
    PLUS("+"),
    MINUS("-"),
    CONCAT("||"),
    MUL("*"),
    DIV("/"), // float/double
    // divide
    MOD("%"),
    CARET("^"),
    IDIV("idiv"), // integer divide
    FUZZY_EQ("~="),
    LIKE("like"),
    NOT_LIKE("not_like"),
    IN("in"),
    NOT_IN("not_in"),
    BETWEEN("between"),
    NOT_BETWEEN("not_between"),
    AVG("avg"),
    MAX("max"),
    MIN("min"),
    SUM("sum"),
    COUNT("count");

    private final String symbol;

    OperatorType(String s) {
        symbol = s;
    }

    @Override
    public String toString() {
        return symbol;
    }

    public static Optional<OperatorType> fromSymbol(String symbol) {
        return Arrays.stream(OperatorType.values()).filter(o -> o.symbol.equals(symbol)).findFirst();
    }
}
