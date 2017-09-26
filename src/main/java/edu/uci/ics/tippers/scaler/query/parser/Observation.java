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

// Parse the observationWiFi.json file
public class Observation {
	private List<String> timestamps;

    public Observation() {
        this.timestamps = new ArrayList<String>();
    }

    public void parseData() {
        // parse json file
        JSONArray observations = new JSONArray();
        try {
            JSONParser parser = new JSONParser();
            observations = (JSONArray) parser.parse(new FileReader("../POST/observationWiFi.json"));
        } catch (FileNotFoundException ex) {
            System.err.println("File not found");
        } catch (IOException ex) {
            System.err.println("Input or out error");
        } catch (ParseException ex) {
            System.err.println("Parse error");
        }
        
        // loop through the observations array and extract data
        for (Object obj : observations) {
            JSONObject observation = (JSONObject) obj;
            String timestamp = (String) observation.get("timestamp");
            if (!this.timestamps.contains(timestamp)) {
                this.timestamps.add(timestamp);
            }
        }
    }

    public List<String> getTimestamps() {
        return timestamps;
    }
}
