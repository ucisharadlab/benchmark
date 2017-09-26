package edu.uci.ics.tippers.scaler.data.trajectory;

import edu.uci.ics.tippers.scaler.data.trajectory.AreaModel;

import java.util.ArrayList;
import java.util.List;

//Path model class to get json data through GSON
public class PathModel {
	String id;
	List<AreaModel> path;

	// Constructor
	public PathModel() {
		path = new ArrayList<AreaModel>();
	}

	// return person path
	public List<AreaModel> getPath() {
		return this.path;
	}

	// return user id
	public String getId() {
		return this.id;
	}
}
