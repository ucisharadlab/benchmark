/*

 Taken From AsterixDB Source Code on GitHub
 */
package edu.uci.ics.tippers.tql.lang.common.statement;

import edu.uci.ics.tippers.tql.lang.common.base.Expression;
import edu.uci.ics.tippers.tql.lang.common.base.Statement;
import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Query implements Statement{
    private boolean topLevel = true;
    private Expression body;
    private int varCounter;
    private List<String> dataverses = new ArrayList<>();
    private List<String> datasets = new ArrayList<>();

    public Query() {

    }

    public Query(boolean explain, boolean topLevel, Expression body, int varCounter, List<String> dataverses,
            List<String> datasets) {
        this.topLevel = topLevel;
        this.body = body;
        this.varCounter = varCounter;
        this.dataverses.addAll(dataverses);
        this.datasets.addAll(datasets);
    }

    public Expression getBody() {
        return body;
    }

    public void setBody(Expression body) {
        this.body = body;
    }

    public int getVarCounter() {
        return varCounter;
    }

    public void setVarCounter(int varCounter) {
        this.varCounter = varCounter;
    }

    public List<Expression> getDirectlyEnclosedExpressions() {
        return Collections.singletonList(body);
    }

    public void setTopLevel(boolean topLevel) {
        this.topLevel = topLevel;
    }

    public boolean isTopLevel() {
        return topLevel;
    }

    public byte getKind() {
        return Statement.Kind.QUERY;
    }

    @Override
    public byte getCategory() {
        return 0;
    }

    public void setDataverses(List<String> dataverses) {
        this.dataverses = dataverses;
    }

    public void setDatasets(List<String> datasets) {
        this.datasets = datasets;
    }

    public List<String> getDataverses() {
        return dataverses;
    }

    public List<String> getDatasets() {
        return datasets;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(body, datasets, dataverses, topLevel);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Query)) {
            return false;
        }
        Query target = (Query) object;
        return ObjectUtils.equals(datasets, target.datasets) && ObjectUtils.equals(dataverses, target.dataverses)
                && topLevel == target.topLevel;
    }

    @Override
    public String toString() {
        return body.toString();
    }
}
