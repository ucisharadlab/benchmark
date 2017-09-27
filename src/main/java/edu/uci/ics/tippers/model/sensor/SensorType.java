package edu.uci.ics.tippers.model.sensor;

import edu.uci.ics.tippers.model.observation.ObservationType;

public class SensorType {

	private String id;

	private String name;

	private String description;

    private String mobility;

	/**
	 * Push/Pull/Streaming
	 */
	private String captureFun;

    private ObservationType observationType;

	private String payloadSchema;
	 
	public SensorType(){
		
	}

	public String getPayloadSchema() {
		return payloadSchema;
	}

	public void setPayloadSchema(String payloadSchema) {
		this.payloadSchema = payloadSchema;
	}

	public SensorType(String name, String description, String payload) {
		this.description = description;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

    public String getCaptureFun() {
		return captureFun;
	}

	public void setCaptureFun(String captureFun) {
		this.captureFun = captureFun;
	}

	public String getId() {
		return id;
	}

    public void setId(String id) {
        this.id = id;
    }

	public String getMobility() {
		return mobility;
	}

	public void setMobility(String mobility) {
		this.mobility = mobility;
	}

	public ObservationType getObservationType() {
        return observationType;
    }

    public void setObservationType(ObservationType observationType) {
        this.observationType = observationType;
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "SensorType [id=" + id + ", description=" + description + ", mobility=" + mobility
                + ", captureFun=" + ((captureFun == null )? "": captureFun.toString()) + "]";
	}

}
