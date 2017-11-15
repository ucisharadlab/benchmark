package edu.uci.ics.tippers.tql.lang.tql;

import edu.uci.ics.tippers.tql.lang.common.base.Statement;

/**
 * Created by peeyush on 11/5/17.
 */
public class SensorToObservation implements Statement{
    private String sensorIdentifier;
    private String observationIdentifier;

    public SensorToObservation(String sensorIdentifier, String observationIdentifier) {
        this.sensorIdentifier= sensorIdentifier;
        this.observationIdentifier = observationIdentifier;
    }

    public String getSensorIdentifier() {
        return sensorIdentifier;
    }

    public void setSensorIdentifier(String sensorIdentifier) {
        this.sensorIdentifier = sensorIdentifier;
    }

    public String getObservationIdentifier() {
        return observationIdentifier;
    }

    public void setObservationIdentifier(String observationIdentifier) {
        this.observationIdentifier = observationIdentifier;
    }

    @Override
    public byte getKind() {
        return Statement.Kind.SENOSR_TO_COLLECTION;
    }

    @Override
    public byte getCategory() {
        return 0;
    }
}
