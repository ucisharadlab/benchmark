package edu.uci.ics.tippers.scaler.data.trajectory;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import edu.uci.ics.tippers.scaler.data.trajectory.AreaObjModel;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//Parse map file
public class TrajectoryParser1 {
	private Map<String, List<String>> map;

	// Constructor
	public TrajectoryParser1() {
		map = new HashMap<String, List<String>>();
	}

	// Parse json file with GSON library
	public void parseData(String filename) throws FileNotFoundException {
		Gson gson = new Gson();
		JsonReader reader = new JsonReader(new FileReader(filename));
		AreaObjModel[] data = gson.fromJson(reader, AreaObjModel[].class); // contains the whole reviews list
	    for (int i = 0, l = data.length; i < l; ++i) {
	    	map.put(data[i].getArea(), data[i].getConnectivity());
	    }
	}

	// Return the area map
	public Map<String, List<String>> getConnectMap() {
		return this.map;
	}
}
