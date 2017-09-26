package edu.uci.ics.tippers.scaler.data.trajectory;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//Parse person path file
public class TrajectoryParser2 {
	private Map<String, Map<String, List<String>>> wifiMap; //(key-area, value - map(key-timestamp, value-list of users))
	private List<String> areas;

	// Constructor
	public TrajectoryParser2() {
		wifiMap = new HashMap<String, Map<String, List<String>>>();
		areas = new ArrayList<String>();
	}

	// Parse json file with GSON library into a map (key-area, value - map(key-timestamp, value-list of users))
	public void parseData(String filename) throws FileNotFoundException {
		// parse file with gson library
		Gson gson = new Gson();
		JsonReader reader = new JsonReader(new FileReader(filename));
		PathModel[] paths = gson.fromJson(reader, PathModel[].class); // contains the whole reviews list

		// Extract information
		for (int i = 0; i < paths.length; ++i) { // loop through the person paths list
			String user = paths[i].getId();
			List<AreaModel> areaList = paths[i].getPath();

			for (int j = 0, l = areaList.size(); j < l; ++j) { // loop through the areas l
				String area = areaList.get(j).getArea();
				String timestamp = areaList.get(j).getTimestamp();

				if (!this.areas.contains(area)) { // create a map for new area and add information in
					this.areas.add(area);
					Map<String, List<String>> map = new HashMap<String, List<String>>();
					List<String> users = new ArrayList<String>();
					users.add(user);
					map.put(timestamp, users);
					wifiMap.put(area, map);
				} else {
					if (wifiMap.get(area).get(timestamp) == null) {
						List<String> users = new ArrayList<String>();
						users.add(user);
						wifiMap.get(area).put(timestamp, users);
					} else {
						wifiMap.get(area).get(timestamp).add(user);
					}
				}
			}
		}
	}

	// Return the area map
	public Map<String, Map<String, List<String>>> getWifiMap() {
		return this.wifiMap;
	}

	// Return the areas as the wifiMap key
	public List<String> getAreas() {
		return this.areas;
	}

}
