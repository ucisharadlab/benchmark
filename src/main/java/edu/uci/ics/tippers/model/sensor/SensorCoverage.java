package edu.uci.ics.tippers.model.sensor;

import edu.uci.ics.tippers.model.metadata.infrastructure.Infrastructure;

import java.util.List;

public class SensorCoverage {

	private String id;
	
	/** The radius. */
	private float radius;
	
	/** The infrastructure covered. */
	private List<Infrastructure> entitiesCovered;

	public SensorCoverage(List<Infrastructure> entitiesCovered) {
		super();
		this.entitiesCovered = entitiesCovered;
	}

	public SensorCoverage(){
		super();
	}

	public float getRadius() {
		return radius;
	}

	public void setRadius(float radius) {
		this.radius = radius;
	}

	public List<Infrastructure> getEntitiesCovered() {
		return entitiesCovered;
	}

	public void setEntitiesCovered(List<Infrastructure> entitiesCovered) {
		this.entitiesCovered = entitiesCovered;
	}

	public String getId() {
		return id;
	}

    public void setId(String id) {
        this.id = id;
    }

}
