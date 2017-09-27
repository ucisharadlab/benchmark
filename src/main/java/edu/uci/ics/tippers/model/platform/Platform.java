package edu.uci.ics.tippers.model.platform;

import edu.uci.ics.tippers.model.metadata.infrastructure.Infrastructure;
import edu.uci.ics.tippers.model.metadata.spatial.Location;
import edu.uci.ics.tippers.model.metadata.user.User;
import edu.uci.ics.tippers.model.sensor.Sensor;

import java.util.List;

/**
 * The Class Platform to model platforms containing sensors (e.g., a smartphone
 * or a raspberry pi)
 */
public class Platform {
	
	private String id;

	private String name;

	private List<Sensor> sensors;

	private User owner;

	private String description;

	private Location location;

	private Infrastructure infrastructure;

	private PlatformType type;

    private PlatformConfig config;

	public Platform(User owner, Location location, Infrastructure infrastructure) {
		this.owner = owner;
		this.location = location;
		this.infrastructure = infrastructure;
	}
	

	public Platform(User owner, Location location, Infrastructure infrastructure, PlatformType type) {
		this.owner = owner;
		this.location = location;
		this.infrastructure = infrastructure;
		this.type = type;
	}
	
	public Platform(){
		
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public User getOwner() {
		return owner;
	}

	public void setOwner(User owner) {
		this.owner = owner;
	}

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}

	/* TODO : Checks whether a user existed in database */
	public boolean exists() {
		return false;
	}

	public PlatformType getType() {
		return type;
	}

	public void setType(PlatformType type) {
		this.type = type;
	}

	public Infrastructure getInfrastructure() {
		return infrastructure;
	}

	public void setInfrastructure(Infrastructure infrastructure) {
		this.infrastructure = infrastructure;
	}

    public PlatformConfig getConfig() {
        return config;
    }

    public void setConfig(PlatformConfig config) {
        this.config = config;
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Sensor> getSensors() {
		return sensors;
	}

	public void setSensors(List<Sensor> sensors) {
		this.sensors = sensors;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
    public String toString() {
        return "Platform [id=" + id +
                ", type=" + type +
                ", location=" + ((location == null)? "null" :location.toString()) +
                ", owner=" + ((owner == null)? "null":owner.getId()) +
                "]";
    }

}
