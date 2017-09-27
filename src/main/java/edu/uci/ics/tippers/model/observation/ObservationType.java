package edu.uci.ics.tippers.model.observation;

public class ObservationType {

    private String id;

	private String name;

	private String description;
	
	private String payloadSchema;
	
	public ObservationType() {
		
	}

	public ObservationType(String name, String description, String payloadSchema) {
		super();
		this.description = description;
		this.payloadSchema = payloadSchema;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getPayloadSchema() {
		return payloadSchema;
	}

	public void setPayloadSchema(String payloadSchema) {
		this.payloadSchema = payloadSchema;
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
}
