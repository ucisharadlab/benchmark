/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.common.expressions;

public interface IExpressionAnnotation {
    public Object getObject();

    public void setObject(Object object);

    public IExpressionAnnotation copy();
}
