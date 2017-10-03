package edu.uci.ics.tippers.model.metadata;


import javax.xml.bind.annotation.XmlType;
import java.util.List;

@XmlType(name="")
public abstract class SemanticEntity {

    protected String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}
