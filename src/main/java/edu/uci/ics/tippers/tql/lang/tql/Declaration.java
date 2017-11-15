package edu.uci.ics.tippers.tql.lang.tql;

import edu.uci.ics.tippers.tql.lang.common.base.Statement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by peeyush on 7/5/17.
 */
public class Declaration implements Statement {

    private List<String> identifiers = new ArrayList<>();
    private String type;

    public Declaration(List<String> identifiers, String type) {
        this.identifiers = identifiers;
        this.type = type;
    }

    public List<String> getIdentifiers() {
        return identifiers;
    }

    public void setIdentifiers(List<String> identifiers) {
        this.identifiers = identifiers;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public byte getKind() {
        return Kind.DECLARATION;
    }

    @Override
    public byte getCategory() {
        return 0;
    }
}
