package edu.uci.ics.tippers.model.metadata.infrastructure;

import edu.uci.ics.tippers.model.metadata.SemanticEntity;
import edu.uci.ics.tippers.model.metadata.spatial.Region;

import javax.xml.bind.annotation.XmlType;

@XmlType(name="")
public class Infrastructure extends SemanticEntity {

	private String name;

	private InfrastructureType type_;

	private InfraConfig infraConfig;

	private Region region;
	
	public Infrastructure() {
		super();
	}

	public InfrastructureType getType_() {
		return type_;
	}

	public void setType_(InfrastructureType type_) {
		this.type_ = type_;
	}

	public InfraConfig getInfraConfig() {
		return infraConfig;
	}

	public void setInfraConfig(InfraConfig infraConfig) {
		this.infraConfig = infraConfig;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Region getRegion() {
		return region;
	}

	public void setRegion(Region region) {
		this.region = region;
	}
}
