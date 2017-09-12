package edu.uci.ics.tippers.scaler.ScaleData;// Scale for trajectory data

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class TrajectoryScale {
	private String outputFilename;
	private Helper helper;
	
	// Constructor
	public TrajectoryScale() {
		outputFilename = "simulatedObs.json";
		helper = new Helper();
	}
	
	// Update filename
	public void setOutputFilename (String outputFilename) {
		this.outputFilename = outputFilename;
	}
	
	// timeInterval is minutes
	public void generatePersonPaths(int personNum, int timeInterval) throws FileNotFoundException {
		Random rand = new Random(); // set up random seed
		
		// parse trajectory json file
		TrajectoryParser1 traj = new TrajectoryParser1();
		traj.parseData(this.outputFilename);
		Map<String, List<String>> connectMap = traj.getConnectMap();
		
		String startTime = "2017-07-11 00:00:00";
		int obsNumPerDay = 24 * 60 / timeInterval; // observation number per day
		
		Set<String> keys = connectMap.keySet(); // get keys
		String[] keyArray = keys.toArray(new String[keys.size()]); // convert set to array
		int keySize = keyArray.length;
		
	    // Write the updated information to new file
	    JsonWriter jsonWriter = null;
		try {
		  jsonWriter = new JsonWriter(new FileWriter(this.outputFilename));
		  jsonWriter.setIndent("  ");
		  jsonWriter.beginArray();
		  for (int i = 1; i <= personNum; ++i) {
			  jsonWriter.beginObject();
			  jsonWriter.name("id");
			  jsonWriter.value("Person" + i);
			  jsonWriter.name("path");
			  jsonWriter.beginArray();
			  int keyIndex = rand.nextInt(keySize); 
			  String area = keyArray[keyIndex]; // get random area
			  String timestamp = startTime;
		  
			  for (int j = 0; j < obsNumPerDay; ++j) {	  
				  
				  jsonWriter.beginObject();
				  jsonWriter.name("area");
				  jsonWriter.value(area);
				  jsonWriter.name("timestamp");
				  jsonWriter.value(timestamp);
				  jsonWriter.endObject();
				  
				  
				  timestamp = helper.increaseTime(timestamp, obsNumPerDay);
				  
				  // get next nearby area
				  List<String> connectsOfArea = connectMap.get(area);
				  connectsOfArea.add(area); // add itself
				  int connectSize = connectsOfArea.size();
				  area = connectsOfArea.get(rand.nextInt(connectSize));
			  }
			  jsonWriter.endArray();
			  jsonWriter.endObject();
		  }
		  
		  jsonWriter.endArray(); // close the json array
		} catch (IOException e) {
		  System.out.println("IO error");
		}finally{
		  try {
		      jsonWriter.close();
		  } catch (IOException e) {
		  	 System.out.println("IO error");
		  }
		}
	}
	
	// Generate wifiAP json file 
	public void generateWifiAP(int timeInterval) throws FileNotFoundException { // TODO: get timeInterval from trajectoryObs
		// parse trajectory json file
		TrajectoryParser2 traj = new TrajectoryParser2();
		traj.parseData(this.outputFilename);
		Map<String, Map<String, List<String>>> wifiMap = traj.getWifiMap();
		List<String> areas = traj.getAreas();
		Collections.sort(areas);
		
		String startTime = "2017-07-11 00:00:00";
		int obsNumPerDay = 24 * 60 / timeInterval; // observation number per day
		String timestamp = startTime;
		
		// Write the updated information to new file
	    JsonWriter jsonWriter = null;
		try {
		    jsonWriter = new JsonWriter(new FileWriter("SimulatedData/wifiAP.json"));
		    jsonWriter.setIndent("  ");
		    jsonWriter.beginArray();
			for (int i = 0; i < obsNumPerDay; ++i) {
				for (String area : areas) {
					List<String> users = wifiMap.get(area).get(timestamp);
					if (users != null) { // only write area that has users
						jsonWriter.beginObject();
						jsonWriter.name("id");
						jsonWriter.value(area);
						jsonWriter.name("timestamp");
						jsonWriter.value(timestamp);
						jsonWriter.name("users");
						jsonWriter.beginArray();
						for (int j = 0, l = users.size(); j < l; ++j) {
							jsonWriter.value(users.get(j));
						}
						jsonWriter.endArray();
						jsonWriter.endObject();
					}
				}
				timestamp = helper.increaseTime(timestamp, obsNumPerDay);
			}
			jsonWriter.endArray();
		} catch (IOException e) {
			System.out.println("IO error");
		} finally {
			try {
			    jsonWriter.close();
			} catch (IOException e) {
			  	System.out.println("IO error");
			}
		}	
	}
}

//Parse person path file
class TrajectoryParser2 {
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

//Path model class to get json data through GSON
class PathModel {
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

//Area Model used by path model
class AreaModel {
	private String area;
	private String timestamp;
	
	public String getArea() {
		return this.area;
	}
	
	public String getTimestamp() {
		return this.timestamp;
	}
}

//Parse map file
class TrajectoryParser1 {
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


//AreaOjjModel to help parse graph with GSON
class AreaObjModel {
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
