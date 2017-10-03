package edu.uci.ics.tippers.data.griddb.mappings;

import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.data.griddb.GridDBBaseDataMapping;
import edu.uci.ics.tippers.model.observation.Observation;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;

public class GridDBDataMapping1 extends GridDBBaseDataMapping {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private JSONParser parser = new JSONParser();

    public GridDBDataMapping1(GridStore gridStore, String dataDir) {
        super(gridStore, dataDir);
        //sdf.setTimeZone(TimeZone.getTimeZone("PDT"));
    }

    public void addAll() throws GSException {
        addMetadata();
        addDevices();
        // addPresenceAndOccupancyData();
        addSensorsAndObservations();
    }

    public void addMetadata() throws GSException {

        Collection<String, Row> collection;
        Row row;
        try {

            // Adding Groups
            JSONArray group_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.GROUP.getPath())));

            for(int i =0;i<group_list.size();i++){
                JSONObject temp=(JSONObject)group_list.get(i);
                collection = gridStore.getCollection("Group");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("description"));

                collection.put(row);
            }

            // Adding Users
            JSONArray user_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.USER.getPath())));

            for(int i =0;i<user_list.size();i++){
                JSONObject temp=(JSONObject)user_list.get(i);
                JSONArray groups;
                collection = gridStore.getCollection("User");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("emailId"));
                row.setValue(2, temp.get("name"));
                row.setValue(3, temp.get("googleAuthToken"));
                groups=(JSONArray)temp.get("groups");
                String[] groups_s=new String[groups.size()];
                for(int x=0;x<groups.size();x++)
                    groups_s[x]=((JSONObject)groups.get(x)).get("id").toString();
                row.setValue(4,groups_s);

                collection.put(row);
            }

            // Adding Locations
            JSONArray location_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.LOCATION.getPath())));

            for(int i =0;i<location_list.size();i++){
                JSONObject temp=(JSONObject)location_list.get(i);
                collection = gridStore.getCollection("Location");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, ((Number)temp.get("x")).doubleValue());
                row.setValue(2, ((Number)temp.get("y")).doubleValue());
                row.setValue(3, ((Number)temp.get("z")).doubleValue());
                collection.put(row);
            }

            // Adding Infrastructure Type
            JSONArray infraType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA_TYPE.getPath())));

            for(int i =0;i<infraType_list.size();i++){
                JSONObject temp=(JSONObject)infraType_list.get(i);
                collection = gridStore.getCollection("InfrastructureType");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("description"));

                collection.put(row);
            }

            // Adding Infrastructure
            JSONArray infra_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA.getPath())));

            for(int i =0;i<infra_list.size();i++){
                JSONObject temp=(JSONObject)infra_list.get(i);
                collection = gridStore.getCollection("Infrastructure");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, ((JSONObject)temp.get("type_")).get("id"));
                row.setValue(3, ((Number)temp.get("floor")).intValue());
                JSONArray locations = (JSONArray)temp.get("geometry");
                String[] locations_s=new String[locations.size()];
                for(int x=0; x<locations.size(); x++)
                    locations_s[x] = ((JSONObject)locations.get(x)).get("id").toString();
                row.setValue(4, locations_s);

                collection.put(row);
            }

        }
        catch(ParseException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void addSensorsAndObservations() throws GSException {
        Collection<String, Row> collection;
        Row row;
        try {
            // Adding Sensor Types
            JSONArray sensorType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR_TYPE.getPath())));

            for(int i =0;i<sensorType_list.size();i++){
                JSONObject temp=(JSONObject)sensorType_list.get(i);
                collection = gridStore.getCollection("SensorType");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("description"));
                row.setValue(3, temp.get("mobility"));

                collection.put(row);
            }

            // Adding Sensors
            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR.getPath())));

            for(int i =0;i<sensor_list.size();i++){
                JSONObject temp=(JSONObject)sensor_list.get(i);
                JSONArray locations;
                collection = gridStore.getCollection("Sensor");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, ((JSONObject)temp.get("type_")).get("id"));
                row.setValue(3, ((JSONObject)temp.get("infrastructure")).get("id"));
                row.setValue(4, ((JSONObject)temp.get("owner")).get("id"));

                JSONArray entitiesCovered = (JSONArray)temp.get("coverage");
                String[] entities=new String[entitiesCovered.size()];
                for(int x=0; x<entitiesCovered.size();x++)
                    entities[x]=((JSONObject)entitiesCovered.get(x)).get("id").toString();
                row.setValue(5, entities);
                row.setValue(6, temp.get("sensorConfig"));

                collection.put(row);

            }

            // Adding Observations
            BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
                    Observation.class);
            Observation obs = null;

            while ((obs = reader.readNext()) != null) {
                String collectionName = Constants.GRIDDB_OBS_PREFIX + obs.getSensor().getId();

                TimeSeries<Row> timeSeries = gridStore.getTimeSeries(collectionName);

                row = timeSeries.createRow();
                row.setValue(0, obs.getTimeStamp());
                row.setValue(1, obs.getId());

                if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                    row.setValue(2, obs.getPayload().get("temperature").getAsInt());
                } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                    row.setValue(2, obs.getPayload().get("clientId").getAsString());
                } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                    row.setValue(2, obs.getPayload().get("currentMilliWatts").getAsInt());
                    row.setValue(3, obs.getPayload().get("onTodaySeconds").getAsInt());
                }
                timeSeries.put(row);
            }

        }
        catch(ParseException | IOException e) {
            e.printStackTrace();
        }


    }

    public void addDevices() throws GSException {

        Collection<String, Row> collection;
        Row row;

        try {
            // Adding PlatformTypes
            JSONArray platformType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT_TYPE.getPath())));

            for(int i =0;i<platformType_list.size();i++){
                JSONObject temp=(JSONObject)platformType_list.get(i);
                collection = gridStore.getCollection("PlatformType");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("description"));

                collection.put(row);
            }

            // Adding Platforms
            JSONArray platform_list = (JSONArray)parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT.getPath())));

            for(int i =0;i<platform_list.size();i++){
                JSONObject temp=(JSONObject)platform_list.get(i);
                collection = gridStore.getCollection("Platform");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("hashedMac"));
                row.setValue(3, ((JSONObject)temp.get("owner")).get("id"));
                row.setValue(4, ((JSONObject)temp.get("type_")).get("id"));
                collection.put(row);
            }

        }
        catch(ParseException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public void addPresenceAndOccupancyData() throws GSException {
//
//        Collection<String, Row> collection;
//        Row row;
//
//        try {
//            ClassLoader classLoader = getClass().getClassLoader();
//            jsonObject = (JSONObject) parser.parse(new InputStreamReader(
//                    classLoader.getResourceAsStream("Presence_dummy_data")));
//
//            // Adding Presence
//            JSONArray presence_list = (JSONArray) jsonObject.get("Presence");
//
//            for(int i =0;i<presence_list.size();i++){
//                JSONObject temp=(JSONObject)presence_list.get(i);
//                collection = gridStore.getCollection("Presence");
//
//                row = collection.createRow();
//                row.setValue(0, temp.get("id"));
//                row.setValue(1, temp.get("locationid"));
//                row.setValue(2, temp.get("userid"));
//                row.setValue(3, temp.get("timestamp"));
//                collection.put(row);
//            }
//
//            // Adding Occupancy
//            jsonObject = (JSONObject) parser.parse(new InputStreamReader(
//                    classLoader.getResourceAsStream("Occupancy_dummy_data")));
//
//            // Adding Presence
//            JSONArray occupancy_list = (JSONArray) jsonObject.get("Occupancy");
//
//            for(int i =0;i<occupancy_list.size();i++){
//                JSONObject temp=(JSONObject)occupancy_list.get(i);
//                collection = gridStore.getCollection("Occupancy");
//
//                row = collection.createRow();
//                row.setValue(0, temp.get("id"));
//                row.setValue(1, temp.get("infraid"));
//                row.setValue(2, temp.get("occupancy"));
//                row.setValue(3, temp.get("timestamp"));
//                collection.put(row);
//            }
//
//        }
//        catch(ParseException e) {
//            e.printStackTrace();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

//    public void addVSAndSemanticObservations() throws GSException {
//
//        Collection<String, Row> collection;
//        Row row;
//
//        // Adding SemanticObservationTypes
//        collection = gridStore.getCollection("User");
//        row = collection.createRow();
//        row.setValue(0, "user1");
//        row.setValue(1, "peeyushg@uci.edu");
//        row.setValue(2, "Peeyush");
//        row.setValue(3, new String[]{"group1"});
//
//        collection.put(row);
//
//        // Adding Virtual Sensor Types
//        collection = gridStore.getCollection("User");
//        row = collection.createRow();
//        row.setValue(0, "user1");
//        row.setValue(1, "peeyushg@uci.edu");
//        row.setValue(2, "Peeyush");
//        row.setValue(3, new String[]{"group1"});
//
//        collection.put(row);
//
//        // Adding Virtual Sensors
//        collection = gridStore.getCollection("User");
//        row = collection.createRow();
//        row.setValue(0, "user1");
//        row.setValue(1, "peeyushg@uci.edu");
//        row.setValue(2, "Peeyush");
//        row.setValue(3, new String[]{"group1"});
//
//        collection.put(row);
//
//        //  TODO: Adding dynamic semantic observations
//
//    }

}

