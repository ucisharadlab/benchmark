package edu.uci.ics.tippers.scaler.data.real;// Scale for real value data

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import edu.uci.ics.tippers.common.util.Helper;

import com.google.gson.stream.JsonWriter;

// Scale for real number data
public class RealValueScale {
	private String outputFilename;
	private Helper helper;
	
	// Constructor
	public RealValueScale() {
		outputFilename = "simulatedObs.json";
		helper = new Helper();
	}
	
	public void setOutputFilename (String outputFilename) {
		this.outputFilename = outputFilename;
	}
	
	// Temporal scaling -- where we extend the data from same sensors for a long time
	public void timeScale(double timeScaleNoise, int extendDays) throws FileNotFoundException {
		// parse real value observation file
		RealValObsParser realValObs = new RealValObsParser();
		realValObs.parseData(this.outputFilename);
		List<String> sensorTypeIds = realValObs.getTypeIds();
		String sensorType = sensorTypeIds.get(0);
		List<String> sensorIds = realValObs.getSensorIds();
		List<Double> payloads = realValObs.getPayloads();
        List<String> timestamps = realValObs.getTimestamps();
        int obsSpeed = realValObs.getObsSpeed();
        int recordDays = realValObs.getRecordDays();
        String payloadName = realValObs.getPayloadName();
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
					  double payload = payloads.get(pastObs+i*sensorSize+j);
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
					  double payload = getRandAroundPayload(payloads.get(i*sensorSize+j), timeScaleNoise);
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
		RealValObsParser realValObs = new RealValObsParser();
		realValObs.parseData(this.outputFilename);
		List<String> sensorTypeIds = realValObs.getTypeIds();
		String sensorType = sensorTypeIds.get(0);
		List<String> sensorIds = realValObs.getSensorIds();
		List<Double> payloads = realValObs.getPayloads();
        List<String> timestamps = realValObs.getTimestamps();
        int recordDays = realValObs.getRecordDays();
        int obsSpeed = realValObs.getObsSpeed();
        String payloadName = realValObs.getPayloadName();
        
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
					  double payload = payloads.get(j+i*sensorSize);
					  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j), payloadName);
					  System.out.println("SpeedScale" + count++); // debug
				  }
				  
				  timestamp = helper.increaseTime(timestamp, scaleSpeed); // increase time based on the observation speed
				  
				  // add simulated observations between two observations for each sensor
				  for (int k = 0; k < speedScaleNum - 1; ++k) {
					  for (int j = 0; j < sensorSize; ++j) {
						  // get random temperature
						  double payload = getRandBetweenPayloads(payloads.get(i*sensorSize+j), payloads.get(i*sensorSize+j+sensorSize), speedScaleNoise);
						  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j), payloadName);
						  System.out.println("SpeedScale" + count++); // debug
					  }
					  timestamp = helper.increaseTime(timestamp, scaleSpeed); // increase time based on the observation speed
				  }  
			  }
			  
			  // handle the end timestamps for all sensors
			  for (int j = 0; j < sensorSize; ++j) {
				  double payload = payloads.get((obsSpeed-1)*sensorSize + j); 
				  jsonWriter = helpWriteToFile(jsonWriter, sensorType, timestamp, payload, sensorIds.get(j), payloadName);
				  System.out.println("SpeedScale" + count++); // debug
			  }
			  
			  
			  // add simulated observations between two observations for each sensor
			  for (int k = 0; k < speedScaleNum - 1; ++k) {
				  timestamp = helper.increaseTime(timestamp, scaleSpeed); // increase time based on the observation speed
				  for (int j = 0; j < sensorSize; ++j) {
					  // get random temperature
					  double payload = getRandAroundPayload(payloads.get((obsSpeed-1)*sensorSize + j), speedScaleNoise);
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
		// parse real value observation file
		RealValObsParser realValObs = new RealValObsParser();
		realValObs.parseData(this.outputFilename);
		List<String> sensorTypeIds = realValObs.getTypeIds();
		String sensorType = sensorTypeIds.get(0);
		List<String> sensorIds = realValObs.getSensorIds();
		List<Double> payloads = realValObs.getPayloads();
        List<String> timestamps = realValObs.getTimestamps();
        int obsSpeed = realValObs.getObsSpeed();
        int recordDays = realValObs.getRecordDays();
        String payloadName = realValObs.getPayloadName();
        
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
					  double payload = 0.0;
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
	public JsonWriter helpWriteToFile(JsonWriter jsonWriter, String sensorType, String timestamp, double payload, String sensorId, String payloadName) {
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
	
	
	// Get random payload from orignal payload by adding some percent of noise
	public double  getRandAroundPayload(double payload, double scaleNoise) {
		Random rand = new Random(); // set up random seed
		double min = payload * (1 - scaleNoise);
		double max = payload * (1 + scaleNoise);
		double result = min + (max - min) * rand.nextDouble();
		result = (double)Math.round(result * 10000d) / 10000d;
		return result;
	}
	
	
	// Generate random number between two real payloads within certain percent of range
	public double getRandBetweenPayloads(double payload1, double payload2, double scaleNoise) {
		Random rand = new Random();
		double min = 0.0;
		double max = 0.0;
		if (payload1 < payload2) {
			min = payload1 * (1 - scaleNoise);
			max = payload2 * (1 + scaleNoise);
		} else {
			min = payload2 * (1 - scaleNoise);
			max = payload2 * (1 + scaleNoise);
		}
		
		double result = min + (max - min) * rand.nextDouble();
		result = (double)Math.round(result * 10000d) / 10000d;
		return result;
	}

}

