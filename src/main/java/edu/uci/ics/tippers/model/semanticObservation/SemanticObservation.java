package edu.uci.ics.tippers.model.semanticObservation;

import com.google.gson.JsonObject;
import edu.uci.ics.tippers.model.virtualSensor.VirtualSensor;

import java.util.Calendar;
import java.util.Date;

public class SemanticObservation {

	private String id;

	private SemanticObservationType type_;

	private JsonObject payload;
	
	private Date timeStamp;
	
	private JsonObject semanticEntity;

	private VirtualSensor virtualSensor;


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public SemanticObservationType getType_() {
		return type_;
	}

	public void setType_(SemanticObservationType type_) {
		this.type_ = type_;
	}

	public Date getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Date timeStamp) {
		this.timeStamp = timeStamp;
	}

	public VirtualSensor getVirtualSensor() {
		return virtualSensor;
	}

	public void setVirtualSensor(VirtualSensor virtualSensor) {
		this.virtualSensor = virtualSensor;
	}

	public JsonObject getSemanticEntity() {
		return semanticEntity;
	}

	public void setSemanticEntity(JsonObject semanticEntity) {
		this.semanticEntity = semanticEntity;
	}

	public JsonObject getPayload() {
		return payload;
	}

	public void setPayload(JsonObject payload) {
		this.payload = payload;
	}
}
