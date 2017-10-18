package edu.uci.ics.tippers.model.virtualSensor;

public class VirtualSensor {
	
	public VirtualSensor() {
		super();
	}

	private String id;
	
	private String name;
	
	private String description;
	
	private String language;
	
	private String projectName;

	private VirtualSensorType type_;

	public VirtualSensorType getType_() {
		return type_;
	}

	public void setType_(VirtualSensorType type_) {
		this.type_ = type_;
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

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

}
