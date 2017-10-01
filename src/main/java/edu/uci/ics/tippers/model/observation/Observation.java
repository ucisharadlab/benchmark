package edu.uci.ics.tippers.model.observation;

import com.google.gson.JsonObject;
import edu.uci.ics.tippers.model.sensor.Sensor;

import java.util.Date;

public class Observation {

	private String id;

	private Sensor sensor;

	private JsonObject payload;
	
	private Date timeStamp;

    private ObservationType type_;

    public Observation(Sensor sensor, JsonObject observationPayload, Date timeStamp, ObservationType type_) {
        super();
        this.sensor = sensor;
        this.payload = observationPayload;
        this.timeStamp = timeStamp;
        this.type_ = type_;
    }

    public Observation(){

    }

	public ObservationType getType_() {
		return type_;
	}

	public void setType_(ObservationType type_) {
		this.type_ = type_;
	}

	public Sensor getSensor() {
		return sensor;
	}

	public void setSensor(Sensor sensor) {
		this.sensor = sensor;
	}

	public JsonObject getPayload() {
		return payload;
	}

	public void setPayload(JsonObject observationPayload) {
		this.payload = observationPayload;
	}

	public Date getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Date timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getId() {
		return id;
	}

    public void setId(String id) {
        this.id = id;
    }

    public ObservationType getObservationType() {
        return type_;
    }

    public void setObservationType(ObservationType observationType) {
        this.type_ = observationType;
    }

    @Override
	public String toString() {
		return "Observation [id=" + id + ", sensor=" + sensor.toString() + ", payload=" + payload
		+"]";
	}

}
