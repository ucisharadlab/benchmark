package edu.uci.ics.tippers.model.observation;

import edu.uci.ics.tippers.model.sensor.Sensor;

import java.util.Calendar;

public class Observation {

	private String id;

	private Sensor sensor;

	private ObservationPayload payload;
	
	private Calendar timeStamp;

    private ObservationType type;

    public Observation(Sensor sensor, ObservationPayload observationPayload, Calendar timeStamp, ObservationType type) {
        super();
        this.sensor = sensor;
        this.payload = observationPayload;
        this.timeStamp = timeStamp;
        this.type = type;
    }

    public Observation(){

    }

	public ObservationType getType() {
		return type;
	}

	public void setType(ObservationType type) {
		this.type = type;
	}

	public Sensor getSensor() {
		return sensor;
	}

	public void setSensor(Sensor sensor) {
		this.sensor = sensor;
	}

	public ObservationPayload getPayload() {
		return payload;
	}

	public void setPayload(ObservationPayload observationPayload) {
		this.payload = observationPayload;
	}

	public Calendar getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Calendar timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getId() {
		return id;
	}

    public void setId(String id) {
        this.id = id;
    }

    public ObservationType getObservationType() {
        return type;
    }

    public void setObservationType(ObservationType observationType) {
        this.type = observationType;
    }

    @Override
	public String toString() {
		return "Observation [id=" + id + ", sensor=" + sensor.toString() + ", payload=" + payload
		+"]";
	}

}
