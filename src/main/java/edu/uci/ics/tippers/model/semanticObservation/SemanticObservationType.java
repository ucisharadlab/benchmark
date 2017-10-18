package edu.uci.ics.tippers.model.semanticObservation;

public class SemanticObservationType {

	private String id;
	
	private String name;	
	
	private String description;
	
	private String payloadSchema;

	public SemanticObservationType(String name, String description, String payloadSchema) {
		super();
		this.name = name;
		this.description = description;
		this.payloadSchema = payloadSchema;
	}

	public SemanticObservationType() {
		super();
	}
	
	public String getName() {
		return name;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
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
	public String getPayloadSchema() {
		return payloadSchema;
	}
	public void setPayloadSchema(String payloadSchema) {
		this.payloadSchema = payloadSchema;
	}
	
}
