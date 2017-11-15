package edu.uci.ics.tippers.tql.lang.tql;

import edu.uci.ics.tippers.tql.lang.common.base.Statement;
import edu.uci.ics.tippers.tql.lang.common.statement.Query;

/**
 * Created by peeyush on 11/5/17.
 */
public class NamedCollection implements Statement {

    private String identifer;
    private Query query;

    public NamedCollection(String identifer, Query query){
        this.identifer = identifer;
        this.query = query;
    }

    public String getIdentifer() {
        return identifer;
    }

    public void setIdentifer(String identifer) {
        this.identifer = identifer;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    @Override
    public byte getKind() {
        return Kind.NAMEDCOLLECTION;
    }

    @Override
    public byte getCategory() {
        return 0;
    }
}
