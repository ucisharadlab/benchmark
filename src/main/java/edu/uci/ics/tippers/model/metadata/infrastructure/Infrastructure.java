package edu.uci.ics.tippers.model.metadata.infrastructure;

import edu.uci.ics.tippers.model.metadata.SemanticEntity;

import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

@XmlType(name="")
public class Infrastructure extends SemanticEntity {

	private String name;

	private InfrastructureType type_;

	private List<Location> geometry = new ArrayList<>();

	private int floor;

	public Infrastructure() {
		super();
	}

	public InfrastructureType getType_() {
		return type_;
	}

	public void setType_(InfrastructureType type_) {
		this.type_ = type_;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Location> getGeometry() {
		return geometry;
	}

	public void setGeometry(List<Location> geometry) {
		this.geometry = geometry;
	}

	public int getFloor() {
		return floor;
	}

	public void setFloor(int floor) {
		this.floor = floor;
	}
}
