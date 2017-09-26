package edu.uci.ics.tippers.scaler.data.trajectory;

import java.util.ArrayList;
import java.util.List;

//AreaOjjModel to help parse graph with GSON
public class AreaObjModel {
	private String area;
	private List<String> connectivity;

	// Constructor
	public AreaObjModel() {
		this.connectivity = new ArrayList<String>();
	}

	public String getArea() {
		return this.area;
	}

	public List<String> getConnectivity() {
		return this.connectivity;
	}
}
