package edu.uci.ics.tippers.scaler.data.counter;// Scale for counter data (int)

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import edu.uci.ics.tippers.common.util.Helper;

import com.google.gson.stream.JsonWriter;

// Counter Scale is for integers
public class CounterScale {
	private String outputFilename;
	private Helper helper;
	
	// Constructor
	public CounterScale() {
		outputFilename = "simulatedObs.json";
		helper = new Helper();
	}
	
	public void setOutputFilename (String outputFilename) {
		this.outputFilename = outputFilename;
	}
	
	// Temporal scaling -- where we extend the data from same sensors for a long time
	public void timeScale(double timeScaleNoise, int extendDays) throws FileNotFoundException {
		// parse temperatureObs file
		CounterObsParser counterObs = new CounterObsParser();
		counterObs.parseData(this.outputFilename);
		List<String> sensorTypeIds = counterObs.getTypeIds();
		String sensorType = sensorTypeIds.get(0);
		List<String> sensorIds = counterObs.getSensorIds();
		List<Integer> payloads = counterObs.getPayloads();
        List<String> timestamps = counterObs.getTimestamps();
        int obsSpeed = counterObs.getObsSpeed();
        int recordDays = counterObs.getRecordDays();
        String payloadName = counterObs.getPayloadName();
		int sensorSize = sensorIds.size();  
        
        // write data to file
        JsonWriter jsonWriter = null;
    	try {
		  jsonWriter = new JsonWriter(new FileWriter(this.outputFilename));
		  jsonWriter.setIndent("  ");
		  jsonWriter.beginArray();
		  
		  int count = 1; // debug
		  
		  // original observations
		  for (int m = 0; m < recordDays; ++m) {
			  int pastObs = m * obsSpeed * sensorSize;
			  for (int i = 0; i < obsSpeed; ++i) { 
				  String timestamp = timestamps.get(i);
				  for (int j = 0; j < sensorSize; ++j) {
					  int payload = payloads.get(pastObs+i*sensorSize+j);
					  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j), payloadName);
					  System.out.println("TimeScale" + count++); // debug
				  }
			  }
		  }
		  
		  // extend days' observations
		  for (int m = 0; m < extendDays; ++m) {
			  int pastDays = recordDays + m;
			  for (int i = 0; i < obsSpeed; ++i) {
				  String timestamp = helper.timeAddDays(timestamps.get(i), pastDays);
				  for (int j = 0; j < sensorSize; ++j) {
					  int payload = getRandAroundPayload(payloads.get(i*sensorSize+j), timeScaleNoise);
					  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j), payloadName);
					  System.out.println("TimeScale" + count++); // debug
				  }
			  }
		  }
		  
		  jsonWriter.endArray(); // close the JSON array
    	  
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
	
	
	/* Speed scaling -- where same devices generate data, but at a faster speed: keep original observations 
    	and add more between two nearby timestamps for each sensor */
	public void speedScale(int speedScaleNum, double speedScaleNoise) throws FileNotFoundException {
		// parse temperatureObs file
		CounterObsParser counterObs = new CounterObsParser();
		counterObs.parseData(this.outputFilename);
		List<String> sensorTypeIds = counterObs.getTypeIds();
		String sensorType = sensorTypeIds.get(0);
		List<String> sensorIds = counterObs.getSensorIds();
		List<Integer> payloads = counterObs.getPayloads();
        List<String> timestamps = counterObs.getTimestamps();
        int recordDays = counterObs.getRecordDays();
        String payloadName = counterObs.getPayloadName();
        int obsSpeed = counterObs.getObsSpeed();
        
        int sensorSize = sensorIds.size();
        int scaleSpeed = obsSpeed * speedScaleNum;
        
        // write data to file
        JsonWriter jsonWriter = null;
    	try {
		  jsonWriter = new JsonWriter(new FileWriter(this.outputFilename));
		  jsonWriter.setIndent("  ");
		  jsonWriter.beginArray();
		  
		  int count = 1; // debug
		  for (int m = 0; m < recordDays; ++m) {
			  String timestamp = helper.timeAddDays(timestamps.get(0), m);
			  for (int i = 0; i < obsSpeed-1; ++i) {  
				  // original observations
				  for (int j = 0; j < sensorSize; ++j) {
					  int payload = payloads.get(j+i*sensorSize);
					  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j), payloadName);
					  System.out.println("SpeedScale" + count++); // debug
				  }
				  
				  timestamp = helper.increaseTime(timestamp, scaleSpeed); // increase time based on the observation speed
				  
				  // add simulated observations between two observations for each sensor
				  for (int k = 0; k < speedScaleNum - 1; ++k) {
					  for (int j = 0; j < sensorSize; ++j) {
						  // get random temperature
						  int payload = getRandBetweenPayloads(payloads.get(i*sensorSize+j), payloads.get(i*sensorSize+j+sensorSize), speedScaleNoise);
						  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j), payloadName);
						  System.out.println("SpeedScale" + count++); // debug
					  }
					  timestamp = helper.increaseTime(timestamp, scaleSpeed); // increase time based on the observation speed
				  }  
			  }
			  
			  // handle the end timestamps for all sensors
			  for (int j = 0; j < sensorSize; ++j) {
				  int payload = payloads.get((obsSpeed-1)*sensorSize + j); 
				  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j), payloadName);
				  System.out.println("SpeedScale" + count++); // debug
			  }
			  
			  // add simulated observations between two observations for each sensor
			  for (int k = 0; k < speedScaleNum - 1; ++k) {
				  timestamp = helper.increaseTime(timestamp, scaleSpeed); // increase time based on the observation speed
				  for (int j = 0; j < sensorSize; ++j) {
					  // get random temperature
					  int payload = getRandAroundPayload(payloads.get((obsSpeed-1)*sensorSize + j), speedScaleNoise);
					  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j), payloadName);
					  System.out.println("SpeedScale" + count++); // debug
				  } 
			  }
		  }
    	  jsonWriter.endArray(); // close the JSON array
    	  
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
	
	
	// device scaling -- when we scale number of devices
	public void deviceScale(int scaleNum, double deviceScaleNoise, String simulatedName) throws FileNotFoundException {
		// parse temperatureObs file
		CounterObsParser counterObs = new CounterObsParser();
		counterObs.parseData(this.outputFilename);
		List<String> sensorTypeIds = counterObs.getTypeIds();
		String sensorType = sensorTypeIds.get(0);
		List<String> sensorIds = counterObs.getSensorIds();
		List<Integer> payloads = counterObs.getPayloads();
        List<String> timestamps = counterObs.getTimestamps();
        int recordDays = counterObs.getRecordDays();
        String payloadName = counterObs.getPayloadName();
        int obsSpeed = counterObs.getObsSpeed();
        
		Random rand = new Random(); // set up random seed
		int sensorSize = sensorIds.size();
		int scaledSensorSize = sensorSize * scaleNum;
		sensorIds = helper.scaleSensorIds(sensorIds, scaleNum, simulatedName);
		
		// write data to file
        JsonWriter jsonWriter = null;
    	try {
		  jsonWriter = new JsonWriter(new FileWriter(this.outputFilename));
		  jsonWriter.setIndent("  ");
		  jsonWriter.beginArray();
		  
		  int count = 1; // debug
		  for (int m = 0; m < recordDays; ++m) {
			  int pastObs = m * obsSpeed * sensorSize;
			  for (int i = 0; i < obsSpeed; ++i) {
				  String timestamp = timestamps.get(m*obsSpeed+i);
				  
				  for (int j = 0; j < scaledSensorSize; ++j) {
					  int payload = 0;
					  if (j < sensorSize) {
						  payload = payloads.get(pastObs+i*sensorSize+j);
					  } else {
						  int n = rand.nextInt(sensorSize);
						  payload = getRandAroundPayload(payloads.get(pastObs+i*sensorSize+n), deviceScaleNoise);
					  }

					  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j), payloadName);
					  
					  System.out.println("DeviceScale" + count++); // debug
				  }
			  }	  
		  }
		  
		  jsonWriter.endArray(); // close the JSON array 
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
	
	
	// edu.uci.ics.tippers.scaler.data.Helper function to help write data to json file
	public JsonWriter helpWriteToFile(JsonWriter jsonWriter, String sensorType, String timestamp, int payload, String sensorId, String payloadName) {
		try {
			jsonWriter.beginObject();
			jsonWriter.name("typeId");
			jsonWriter.value(sensorType);
			jsonWriter.name("timestamp");
			jsonWriter.value(timestamp);
			jsonWriter.name("payload");
			jsonWriter.beginObject();
			jsonWriter.name(payloadName);
			jsonWriter.value(payload);
			jsonWriter.endObject();
			jsonWriter.name("sensorId");
			jsonWriter.value(sensorId);
			jsonWriter.endObject();
		}  catch (IOException e) {
			System.out.println (e.toString());
			System.out.println("IO error");
	    	  
		}

		return jsonWriter;
	}
	
	
	// Get random payload based on real payload within certain percent of noise
	public int getRandAroundPayload(int payload, double scaleNoise) {
		Random rand = new Random(); // set up random seed
		int min = (int) (payload * (1 - scaleNoise));
		int max = (int) (payload * (1 + scaleNoise));
		return rand.nextInt(max-min+1) + min;
	}
	
	
	// Generate random number between two real payloads within certain percent of range
	public int getRandBetweenPayloads(int payload1, int payload2, double scaleNoise) {
		Random rand = new Random();
		if (payload1 < payload2) {
			int min = (int) (payload1 * (1 - scaleNoise));
			int max = (int) (payload2 * (1 + scaleNoise));
			return rand.nextInt(max-min+1) + min;
		} else {
			int min = (int) (payload2 * (1 - scaleNoise));
			int max = (int) (payload2 * (1 + scaleNoise));
			return rand.nextInt(max-min+1) + min;
		}
	}

}


