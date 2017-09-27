package edu.uci.ics.tippers.model.metadata.infrastructure;

public class InfrastructureType {

	private String id;

	private String name;

	private String description;
	
	public InfrastructureType(){}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
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

	@Override
	public String toString() {
		return "InfrastructureType [id=" + id + ", description=" + description +"]";
	}

}

