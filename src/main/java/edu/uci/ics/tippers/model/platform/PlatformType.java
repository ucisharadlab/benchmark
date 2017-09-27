package edu.uci.ics.tippers.model.platform;

public class PlatformType {

	private String id;

	private String name;
	
	private String description;

    public PlatformType(){}
	
	public PlatformType(String id, String name, String description, String imagePath) {
		super();
		this.id = id;
		this.name = name;
		this.description = description;
    }
	
	public PlatformType(String name, String description, String imagePath) {
		super();
		this.name = name;
		this.description = description;
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

    public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "PlatformType [id=" + id + ", name=" + name + ", description=" + description
				+ "]";
	}
}

