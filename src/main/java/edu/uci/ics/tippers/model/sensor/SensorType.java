package edu.uci.ics.tippers.model.sensor;

public class SensorType {

	private String id;

	private String name;

	private String description;

    private String mobility;

	private String captureFunctionality;

	private String payloadSchema;

	public String getPayloadSchema() {
		return payloadSchema;
	}

	public void setPayloadSchema(String payloadSchema) {
		this.payloadSchema = payloadSchema;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

    public String getCaptureFunctionality() {
		return captureFunctionality;
	}

	public void setCaptureFunctionality(String captureFunctionality) {
		this.captureFunctionality = captureFunctionality;
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

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "SensorType [id=" + id + ", description=" + description + ", mobility=" + mobility
                + ", captureFunctionality=" + ((captureFunctionality == null )? "": captureFunctionality) + "]";
	}

}
