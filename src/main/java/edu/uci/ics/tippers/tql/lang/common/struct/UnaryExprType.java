/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.struct;

import java.util.Arrays;
import java.util.Optional;

public enum UnaryExprType {
    POSITIVE("+"),
    NEGATIVE("-"),
    EXISTS("exists"),
    NOT_EXISTS("not_exists");

    private final String symbol;

    UnaryExprType(String s) {
        symbol = s;
    }

    @Override
    public String toString() {
        return symbol;
    }

    public static Optional<UnaryExprType> fromSymbol(String symbol) {
        return Arrays.stream(UnaryExprType.values()).filter(o -> o.symbol.equals(symbol)).findFirst();
    }
}
