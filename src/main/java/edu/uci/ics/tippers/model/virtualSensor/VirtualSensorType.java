package edu.uci.ics.tippers.model.virtualSensor;

import edu.uci.ics.tippers.model.semanticObservation.SemanticObservationType;
import edu.uci.ics.tippers.model.sensor.SensorType;

public class VirtualSensorType {

	private String id;

    private String name;

    private String description;

	public SensorType inputType;

	public SemanticObservationType semanticObservationType;

	public SensorType getInputType() {
		return inputType;
	}

	public void setInputType(SensorType inputType) {
		this.inputType = inputType;
	}

	public SemanticObservationType getSemanticObservationType() {
		return semanticObservationType;
	}

	public void setSemanticObservationType(SemanticObservationType semanticObservationType) {
		this.semanticObservationType = semanticObservationType;
	}

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
