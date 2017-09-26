package edu.uci.ics.tippers.scaler.data.counter;

import edu.uci.ics.tippers.common.util.Helper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

//Counter observation parsing class
public class CounterObsParser
{
	private List<String> typeIds;
	private List<String> sensorIds;
	private List<String> timestamps;
	private List<Integer> payloads;
	private int obsCount; // total number of observation
	private int obsSpeed; // observation count per day and per sensor
	private Helper helper;
	private String payloadName; // Set<String> keys = posts.keyset();
	private List<String> keyList; // sequence: payload, typeId, timestamp, sensorId

	public CounterObsParser() {
		this.typeIds = new ArrayList<String>();
		this.sensorIds = new ArrayList<String>();
		this.timestamps = new ArrayList<String>();
		this.payloads = new ArrayList<Integer>();
		this.obsCount = 0;
		this.obsSpeed = 0;
		helper = new Helper();
		keyList = new ArrayList<String>();
	}

	// Read Json file, parse it, and store the extracted data to related lists
	public void parseData(String filename) {
		// parse json file
     JSONArray observations = new JSONArray();
     try {
         JSONParser parser = new JSONParser();
         observations = (JSONArray) parser.parse(new FileReader(filename));
     } catch (FileNotFoundException ex) {
     	System.out.println (ex.toString());
         System.err.println("File not found");
     } catch (IOException ex) {
     	System.out.println (ex.toString());
         System.err.println("Input or out error");
     } catch (ParseException ex) {
     	System.out.println (ex.toString());
         System.err.println("Parse error");
     }

     int count = 0;
     // loop through the observations array and extract data
     for (Object obj : observations) {
         JSONObject observation = (JSONObject) obj;

         if (count == 0) {
	            @SuppressWarnings("unchecked")
                Set<String> keys = observation.keySet();
	            keyList = new ArrayList<String>(keys);
	            System.out.println(keyList.get(2));
	            JSONObject payloadObj = (JSONObject) observation.get(keyList.get(0));
	            @SuppressWarnings("unchecked")
				Set<String> payloadKey = payloadObj.keySet();
	            ArrayList<String> payload = new ArrayList<String>(payloadKey);
	            this.payloadName = payload.get(0);
	            count++;
         }

         this.obsCount++;        // count the total observations

         // get sensor type IDs
         String typeId = (String) observation.get(keyList.get(1));
         if (!this.typeIds.contains(typeId)) {
				this.typeIds.add(typeId);
			}

			// get sensor ids
			String sensorId = (String) observation.get(keyList.get(3));
         if (!this.sensorIds.contains(sensorId)) {
				this.sensorIds.add(sensorId);
			}

			// get timestamps
			String timestamp = (String) observation.get(keyList.get(2));
         if (!this.timestamps.contains(timestamp)) {
             this.timestamps.add(timestamp);
         }

			// get sensor observation number per day
			Timestamp currTime = Timestamp.valueOf(timestamp);
			Timestamp startTime = Timestamp.valueOf(this.timestamps.get(0));
			Timestamp endTime = Timestamp.valueOf(helper.timeAddDays(this.timestamps.get(0), 1));
			if ((currTime.equals(startTime) || currTime.after(startTime))
				&& currTime.before(endTime) && sensorId.equals(this.sensorIds.get(0))) {
				this.obsSpeed++;
			}

			// get payload
         JSONObject payloadObj = (JSONObject) observation.get(keyList.get(0));
         Integer payload = (int) (long) payloadObj.get(this.payloadName);
         this.payloads.add(payload);
		}
	}


	// return type Ids
	public List<String> getTypeIds() {
		return this.typeIds;
	}

	// return sensor Ids
	public List<String> getSensorIds() {
		return this.sensorIds;
	}

	// return payloads
	public List<Integer> getPayloads() {
		return this.payloads;
	}

	// return the observation number per day per sensor
	public int getObsSpeed() {
		return this.obsSpeed;
	}

	// return the timestamps
	public List<String> getTimestamps() {
		return this.timestamps;
	}

	// return the recorded Days of current observations
	public int getRecordDays() {
		return this.obsCount/(this.sensorIds.size() * this.obsSpeed);
	}

	// return the key list: payload, typeId, timestamp, sensorId
	public List<String> getKeyList() {
		return this.keyList;
	}

	// return payload name
	public String getPayloadName() {
		return this.payloadName;
	}
}
