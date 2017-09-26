package edu.uci.ics.tippers.scaler.query.parser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

// Parse sensorType.json file to extract the sensor type information
public class SensorType {
	private List<String> ids;

    // A constructor to initialize the Data Members for
    public SensorType() {
        this.ids = new ArrayList<String>();
    }

    // Parse the json file and extract the sensor type ids
    public void parseData() {
        // parse json file
        JSONArray sensorTypes = new JSONArray();
        try {
            JSONParser parser = new JSONParser();
            sensorTypes = (JSONArray) parser.parse(new FileReader("../POST/sensorType.json"));
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
        
        // loop through the sensors array and extract ids
        for (Object obj : sensorTypes) {
            JSONObject sensorType = (JSONObject) obj;
            String id = (String) sensorType.get("id");
            this.ids.add(id);
        }
    }

    // return sensor id list
    public List<String> getIds() {
        return this.ids;
    }

}
