package edu.uci.ics.tippers.model.metadata;

import edu.uci.ics.tippers.model.semanticObservation.SemanticObservation;

import javax.xml.bind.annotation.XmlType;
import java.util.List;

@XmlType(name="")
public abstract class SemanticEntity {

    protected String id;

    /** The semantic observations associated with the entity. */
    private List<SemanticObservation> semanticObservations;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}
