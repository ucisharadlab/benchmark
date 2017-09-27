package edu.uci.ics.tippers.model.sensor;

import edu.uci.ics.tippers.model.metadata.infrastructure.Infrastructure;
import edu.uci.ics.tippers.model.metadata.spatial.Location;
import edu.uci.ics.tippers.model.metadata.user.User;
import edu.uci.ics.tippers.model.platform.Platform;

import java.util.ArrayList;

public class Sensor {

	private String id;

	private String name;

	private String description;

	private SensorType sensorType;
	
	private Location location;

    private Infrastructure infrastructure;

	private Platform platform;
	
	private User owner;
	
	private SensorCoverage coverage;

    private SensorConfig sensorConfig;

	public Sensor(String id, String name, String description, String sensorIP, String sensorPort,
                  SensorType sensorType, Location location, Platform platform, User owner) {
		super();
		this.id = id;
		this.sensorType = sensorType;
		this.location = location;
		this.platform = platform;
		this.owner = owner;
	}

	public Sensor(){

	}

    public Sensor(String id, String name, String description, String sensorIP, String sensorPort,
                  SensorType sensorType, Location location, Platform platform,
                  User owner, SensorCoverage coverage) {
        super();
        this.id = id;
		this.sensorType = sensorType;
        this.location = location;
        this.platform = platform;
        this.owner = owner;
        this.coverage = coverage;
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

	public SensorType getSensorType() {
		return sensorType;
	}

	public void setSensorType(SensorType sensorType) {
		this.sensorType = sensorType;
	}

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}

	public Platform getPlatform() {
		return platform;
	}

	public void setPlatform(Platform platform) {
		this.platform = platform;
	}

	public User getOwner() {
		return owner;
	}

	public void setOwner(User owner) {
		this.owner = owner;
	}

	public String getId() {
		return id;
	}

	public SensorCoverage getCoverage() {
		return coverage;
	}

	public void setCoverage(SensorCoverage coverage) {
		this.coverage = coverage;
	}

    public void setId(String id) {
        this.id = id;
    }

    public Infrastructure getInfrastructure() {
        return infrastructure;
    }

    public void setInfrastructure(Infrastructure infrastructure) {
        this.infrastructure = infrastructure;
    }

    public SensorConfig getSensorConfig() {
        return sensorConfig;
    }

    public void setSensorConfig(SensorConfig sensorConfig) {
        this.sensorConfig = sensorConfig;
    }

    @Override
    public String toString() {

        ArrayList<String> coverageIDs = null;
        if( coverage != null ) {
            coverageIDs = new ArrayList<>();
            System.out.println(coverage.getEntitiesCovered().size());

            for(int i=0; i < coverage.getEntitiesCovered().size(); i++)
                coverageIDs.add(coverage.getEntitiesCovered().get(i).getId());
        }

        return "Sensor [id=" + id +
                ", location=" +  ((location == null)? "null":location.toString()) +
                ", type=" + ((sensorType == null)? "null":sensorType.toString()) +
                ", platform=" + ((platform == null)? "null":platform.toString()) +
                ", user_id=" + ((owner == null)? "null":owner.getId()) +
                ", coverage_room_ids=" + ((coverageIDs == null)? "null":coverageIDs.toString()) +
                "]";
    }

}