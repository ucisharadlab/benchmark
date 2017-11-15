/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.common.expressions;

public class IndexedNLJoinExpressionAnnotation extends AbstractExpressionAnnotation {

    public static final String HINT_STRING = "indexnl";
    public static final IndexedNLJoinExpressionAnnotation INSTANCE = new IndexedNLJoinExpressionAnnotation();

    @Override
    public IExpressionAnnotation copy() {
        IndexedNLJoinExpressionAnnotation clone = new IndexedNLJoinExpressionAnnotation();
        clone.setObject(object);
        return clone;
    }

    @Override
    public String toString() {
        return HINT_STRING;
    }
}
