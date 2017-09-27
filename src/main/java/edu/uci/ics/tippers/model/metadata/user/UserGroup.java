package edu.uci.ics.tippers.model.metadata.user;


public class UserGroup {

	private String id;
	
	private String name;
	
	private String description;
	
	public UserGroup() {
		
	}
	
	public UserGroup(String name, String description) {
		this.name = name;
		this.description = description;
	}

	@Override
	public String toString() {
		return "UserGroup [id=" + id + ", name=" + name + ", description=" + description + "]";
	}

	public String getId() {
		return id;
	}

	public String getDescription() {
		return description;
	}
	
	public void setDescription(String description) {
		this.description = description;
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
