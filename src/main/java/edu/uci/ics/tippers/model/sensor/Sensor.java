package edu.uci.ics.tippers.model.sensor;

import edu.uci.ics.tippers.model.metadata.infrastructure.Infrastructure;
import edu.uci.ics.tippers.model.metadata.user.User;

import java.util.ArrayList;
import java.util.List;

public class Sensor {

	private String id;

	private String name;

	private SensorType type_;

	private Infrastructure infrastructure;

	private User owner;

    private String sensorConfig;

	private List<Infrastructure> coverage;

    public List<Infrastructure> getCoverage() {
        return coverage;
    }

    public void setCoverage(List<Infrastructure> coverage) {
        this.coverage = coverage;
    }

    public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public SensorType getType_() {
		return type_;
	}

	public void setType_(SensorType type_) {
		this.type_ = type_;
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

    public void setId(String id) {
        this.id = id;
    }

    public Infrastructure getInfrastructure() {
        return infrastructure;
    }

    public void setInfrastructure(Infrastructure infrastructure) {
        this.infrastructure = infrastructure;
    }

    public String getSensorConfig() {
        return sensorConfig;
    }

    public void setSensorConfig(String sensorConfig) {
        this.sensorConfig = sensorConfig;
    }

    @Override
    public String toString() {

        ArrayList<String> coverageIDs = null;
        if( coverage != null ) {
            coverageIDs = new ArrayList<>();
            System.out.println(coverage.size());

            for(int i=0; i < coverage.size(); i++)
                coverageIDs.add(coverage.get(i).getId());
        }

        return "Sensor [id=" + id +
                ", type=" + ((type_ == null)? "null": type_.toString()) +
                ", user_id=" + ((owner == null)? "null":owner.getId()) +
                ", coverage_room_ids=" + ((coverageIDs == null)? "null":coverageIDs.toString()) +
                "]";
    }

}