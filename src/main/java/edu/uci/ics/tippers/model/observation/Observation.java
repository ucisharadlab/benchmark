package edu.uci.ics.tippers.model.observation;

import com.google.gson.JsonObject;
import edu.uci.ics.tippers.model.sensor.Sensor;

import java.util.Date;

public class Observation {

	private String id;

	private Sensor sensor;

	private JsonObject payload;
	
	private Date timeStamp;

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

    @Override
	public String toString() {
		return "Observation [id=" + id + ", sensor=" + sensor.toString() + ", payload=" + payload
		+"]";
	}

}
