import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class InfrastructureType {
	private List<String> ids;

    // A constructor to initialize the Data Members for
    public InfrastructureType() {
        this.ids = new ArrayList<String>();
    }

    // Parse the json file and extract the semsor ids
    public void parseData() {
        // parse json file
        JSONArray locationTypes = new JSONArray();
        try {
            JSONParser parser = new JSONParser();
            locationTypes = (JSONArray) parser.parse(new FileReader("../POST/sensor.json"));
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
        for (Object obj : locationTypes) {
            JSONObject locationType = (JSONObject) obj;
            String id = (String) locationType.get("id");
            this.ids.add(id);
        }
    }
    
    // return the infrastructureType Ids
    public List<String> getIds() {
    	return this.ids;
    }

}
