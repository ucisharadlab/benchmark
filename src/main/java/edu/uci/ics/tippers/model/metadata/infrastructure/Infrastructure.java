package edu.uci.ics.tippers.model.metadata.infrastructure;

import edu.uci.ics.tippers.model.metadata.SemanticEntity;
import edu.uci.ics.tippers.model.metadata.spatial.Region;

import javax.xml.bind.annotation.XmlType;

@XmlType(name="")
public class Infrastructure extends SemanticEntity {

	private String name;

	private InfrastructureType type;

	private InfraConfig infraConfig;

	private Region region;
	
	public Infrastructure() {
		super();
	}

	public InfrastructureType getType() {
		return type;
	}

	public void setType(InfrastructureType type) {
		this.type = type;
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
