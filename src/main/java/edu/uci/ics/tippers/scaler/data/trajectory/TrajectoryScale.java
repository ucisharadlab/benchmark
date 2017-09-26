package edu.uci.ics.tippers.scaler.data.trajectory;// Scale for trajectory data

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.gson.stream.JsonWriter;
import edu.uci.ics.tippers.common.util.Helper;

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


