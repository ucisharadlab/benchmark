import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

// Parse temperatureObs.json file
public class TemperatureObs 
{
	private List<String> typeIds;
	private List<String> timestamps;
	private List<Integer> payloads;
	
	// Constructor
	public TemperatureObs() {
		this.typeIds = new ArrayList<String>();
		this.timestamps = new ArrayList<String>();
		this.payloads = new ArrayList<Integer>();
	}
	
	public void parseData() {
		// parse json file
        JSONArray observations = new JSONArray();
        try {
            JSONParser parser = new JSONParser();
            observations = (JSONArray) parser.parse(new FileReader("../POST/temperatureObs.json"));
        } catch (FileNotFoundException ex) {
            System.err.println("File not found");
            // exit(1); Problem
        } catch (IOException ex) {
            System.err.println("Input or out error");
            // exit(1);
        } catch (ParseException ex) {
            System.err.println("Parse error");
            // exit(1);
        }
        
        // loop through the observations array and extract data
        for (Object obj : observations) {
            JSONObject observation = (JSONObject) obj;
            String typeId = (String) observation.get("typeId");
            if (!this.typeIds.contains(typeId)) {
            	this.typeIds.add(typeId);
            }
            
            String timestamp = (String) observation.get("timestamp");
            if (!this.timestamps.contains(timestamp)) {
                this.timestamps.add(timestamp);
            }
            
            JSONObject payloadObj = (JSONObject) observation.get("payload");
            Integer payload = (int) (long) payloadObj.get("temperature");
            if (!this.payloads.contains(payload)) {
            	this.payloads.add(payload);
            }   
        }
	}
	
	public List<String> getTypeIds() {
		return this.typeIds;
	}
	
	public List<String> getTimestamps() {
		return this.timestamps;
	}
	
	public List<Integer> getPayloads() {
		return this.payloads;
	}
}
