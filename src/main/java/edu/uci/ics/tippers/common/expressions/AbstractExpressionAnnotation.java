/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.common.expressions;

public abstract class AbstractExpressionAnnotation implements IExpressionAnnotation {

    protected Object object;

    @Override
    public Object getObject() {
        return object;
    }

    @Override
    public void setObject(Object object) {
        this.object = object;
    }

}
