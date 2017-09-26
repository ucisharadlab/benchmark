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

// Parse sensorOld.json file
public class Sensor
{  
    private List<String> ids;
    private List<String> typeIds;
    private List<String> infraStructureIds;

    // A constructor to initialize the Data Members for
    public Sensor() {
        this.ids = new ArrayList<String>();
        this.typeIds = new ArrayList<String>();
        this.infraStructureIds = new ArrayList<String>();
    }

    // Parse the json file and extract the semsor ids
    public void parseData() {
        // parse json file
        JSONArray sensors = new JSONArray();
        try {
            JSONParser parser = new JSONParser();
            sensors = (JSONArray) parser.parse(new FileReader("../POST/sensorOld.json"));
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
        for (Object obj : sensors) {
            JSONObject sensor = (JSONObject) obj;
            String id = (String) sensor.get("id");
            this.ids.add(id);

            String typeId = (String) sensor.get("typeId");
            if (!this.typeIds.contains(typeId)) {
                this.typeIds.add(typeId);
            }

            String infraStructureId = (String) sensor.get("infraStructureId");
            if (!this.infraStructureIds.contains(infraStructureId)) {
                this.infraStructureIds.add(infraStructureId);
            }
        }
    }

    // return sensor id list
    public List<String> getIds() {
        return this.ids;
    }

    // return sensor type id list
    public List<String> getTypeIds() {
        return this.typeIds;
    }

    // return sensor location list
    public List<String> getInfraStructureIds() {
        return this.infraStructureIds;
    }  
}