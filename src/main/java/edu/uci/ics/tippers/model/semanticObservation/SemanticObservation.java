package edu.uci.ics.tippers.model.semanticObservation;

import edu.uci.ics.tippers.model.metadata.SemanticEntity;
import edu.uci.ics.tippers.model.virtualSensor.VirtualSensor;

import java.util.Calendar;
import java.util.List;

public class SemanticObservation {

	private String id;

	private SemanticObservationType type;

	private SemanticPayload payload;
	
	private Calendar timeStamp;
	
	private transient SemanticEntity semanticEntity;

	private VirtualSensor virtualSensor;


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public SemanticObservationType getType() {
		return type;
	}

	public void setType(SemanticObservationType type) {
		this.type = type;
	}

	public SemanticPayload getPayload() {
		return payload;
	}

	public void setPayload(SemanticPayload payload) {
		this.payload = payload;
	}

	public Calendar getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Calendar timeStamp) {
		this.timeStamp = timeStamp;
	}

	public VirtualSensor getVirtualSensor() {
		return virtualSensor;
	}

	public void setVirtualSensor(VirtualSensor virtualSensor) {
		this.virtualSensor = virtualSensor;
	}

	public SemanticEntity getSemanticEntity() {
		return semanticEntity;
	}

	public void setSemanticEntity(SemanticEntity semanticEntity) {
		this.semanticEntity = semanticEntity;
	}
}
