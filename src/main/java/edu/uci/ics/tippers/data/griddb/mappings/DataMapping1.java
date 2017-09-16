package edu.uci.ics.tippers.data.griddb.mappings;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.Row;
import edu.uci.ics.tippers.data.griddb.BaseDataMapping;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class DataMapping1 extends BaseDataMapping {

    private JSONParser parser = new JSONParser();
    private JSONObject jsonObject;

    public DataMapping1(GridStore gridStore, String dataDir) {
        super(gridStore, dataDir);
    }

    public void addAll() throws GSException {
        addMetadata();
        addDevices();
        addPresenceAndOccupancyData();
        addSensorsAndObservations();
    }

    public void addMetadata() throws GSException {

        Collection<String, Row> collection;
        Row row;
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            jsonObject = (JSONObject) parser.parse(new InputStreamReader(
                    classLoader.getResourceAsStream("collection_container_dummy")));

            // Adding Groups
            JSONArray group_list = (JSONArray) jsonObject.get("Group");

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
            JSONArray user_list = (JSONArray) jsonObject.get("User");

            for(int i =0;i<user_list.size();i++){
                JSONObject temp=(JSONObject)user_list.get(i);
                JSONArray groups;
                collection = gridStore.getCollection("User");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("email"));
                row.setValue(2, temp.get("name"));
                row.setValue(3, temp.get("name"));
                groups=(JSONArray)temp.get("group");
                String[] groups_s=new String[groups.size()];
                for(int x=0;x<groups.size();x++)
                    groups_s[x]=groups.get(x).toString();
                row.setValue(4,groups_s);

                collection.put(row);
            }

            // Adding Locations
            JSONArray location_list = (JSONArray) jsonObject.get("Location");

            for(int i =0;i<location_list.size();i++){
                JSONObject temp=(JSONObject)location_list.get(i);
                collection = gridStore.getCollection("Location");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, (Double)temp.get("x"));
                row.setValue(2, (Double)temp.get("y"));
                row.setValue(3, (Double)temp.get("z"));
                collection.put(row);
            }

            // Adding Regions
            JSONArray region_list = (JSONArray) jsonObject.get("Region");

            for(int i =0;i<region_list.size();i++){
                JSONObject temp=(JSONObject)region_list.get(i);
                JSONArray locations;
                collection = gridStore.getCollection("Region");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, (Double)temp.get("floor"));
                row.setValue(2, temp.get("name"));
                locations=(JSONArray)temp.get("geometry");
                String[] locations_s=new String[locations.size()];
                for(int x=0;x<locations.size();x++)
                    locations_s[x]=locations.get(x).toString();
                row.setValue(3,locations_s);

                collection.put(row);
            }

            // Adding Infrastructure Type
            JSONArray infraType_list = (JSONArray) jsonObject.get("InfrastructureType");

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
            JSONArray infra_list = (JSONArray) jsonObject.get("Infrastructure");

            for(int i =0;i<infra_list.size();i++){
                JSONObject temp=(JSONObject)infra_list.get(i);
                collection = gridStore.getCollection("Infrastructure");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("type"));
                row.setValue(3, temp.get("region"));

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
            ClassLoader classLoader = getClass().getClassLoader();
            jsonObject = (JSONObject) parser.parse(new InputStreamReader(
                    classLoader.getResourceAsStream("collection_container_dummy")));


            // Adding Sensor Types
            JSONArray sensorType_list = (JSONArray) jsonObject.get("SensorType");

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

            // Adding ObservationTypes
            JSONArray obsType_list = (JSONArray) jsonObject.get("ObservationType");

            for(int i =0;i<obsType_list.size();i++){
                JSONObject temp=(JSONObject)obsType_list.get(i);
                collection = gridStore.getCollection("ObservationType");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("description"));

                collection.put(row);
            }

            // Adding Sensors
            JSONArray sensor_list = (JSONArray) jsonObject.get("Sensor");

            for(int i =0;i<sensor_list.size();i++){
                JSONObject temp=(JSONObject)sensor_list.get(i);
                JSONArray locations;
                collection = gridStore.getCollection("Sensor");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("description"));
                row.setValue(2, temp.get("location"));
                row.setValue(3, temp.get("type"));
                row.setValue(4, temp.get("observationType"));
                row.setValue(5, temp.get("platform"));
                locations=(JSONArray)temp.get("coverageRooms");
                String[] locations_s=new String[locations.size()];
                for(int x=0;x<locations.size();x++)
                    locations_s[x]=locations.get(x).toString();
                row.setValue(6,locations_s);

                collection.put(row);
            }

            // TODO: Add dynamic Observations
            jsonObject = (JSONObject) parser.parse(new InputStreamReader(
                    classLoader.getResourceAsStream("observation_dummy_data")));

            JSONArray obs_list = (JSONArray) jsonObject.get("TS_Observation_3101_clwa_1101");

            for(int i =0;i<obs_list.size();i++){
                JSONObject temp=(JSONObject)obs_list.get(i);
                collection = gridStore.getCollection("TS_Observation_3101_clwa_1101");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("timestamp"));
                row.setValue(2, temp.get("clientid"));

                collection.put(row);
            }

            obs_list = (JSONArray) jsonObject.get("TS_Observation_3101_clwa_2101");

            for(int i =0;i<obs_list.size();i++){
                JSONObject temp=(JSONObject)obs_list.get(i);
                collection = gridStore.getCollection("TS_Observation_3101_clwa_2101");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("timestamp"));
                row.setValue(2, temp.get("clientid"));

                collection.put(row);
            }

            obs_list = (JSONArray) jsonObject.get("TS_Observation_3101_clwa_3101");

            for(int i =0;i<obs_list.size();i++){
                JSONObject temp=(JSONObject)obs_list.get(i);
                collection = gridStore.getCollection("TS_Observation_3101_clwa_3101");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("timestamp"));
                row.setValue(2, temp.get("clientid"));

                collection.put(row);
            }

            obs_list = (JSONArray) jsonObject.get("TS_Observation_3101_clwa_4101");

            for(int i =0;i<obs_list.size();i++){
                JSONObject temp=(JSONObject)obs_list.get(i);
                collection = gridStore.getCollection("TS_Observation_3101_clwa_4101");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("timestamp"));
                row.setValue(2, temp.get("clientid"));

                collection.put(row);
            }

            obs_list = (JSONArray) jsonObject.get("TS_Observation_3101_clwa_5101");

            for(int i =0;i<obs_list.size();i++){
                JSONObject temp=(JSONObject)obs_list.get(i);
                collection = gridStore.getCollection("TS_Observation_3101_clwa_5101");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("timestamp"));
                row.setValue(2, temp.get("clientid"));

                collection.put(row);
            }

            obs_list = (JSONArray) jsonObject.get("TS_Observation_3101_clwa_6101");

            for(int i =0;i<obs_list.size();i++){
                JSONObject temp=(JSONObject)obs_list.get(i);
                collection = gridStore.getCollection("TS_Observation_3101_clwa_6101");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("timestamp"));
                row.setValue(2, temp.get("clientid"));

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

    public void addDevices() throws GSException {

        Collection<String, Row> collection;
        Row row;

        try {
            ClassLoader classLoader = getClass().getClassLoader();
            jsonObject = (JSONObject) parser.parse(new InputStreamReader(
                    classLoader.getResourceAsStream("collection_container_dummy")));

            // Adding PlatformTypes
            JSONArray platformType_list = (JSONArray) jsonObject.get("PlatformType");

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
            JSONArray platform_list = (JSONArray) jsonObject.get("Platform");

            for(int i =0;i<platform_list.size();i++){
                JSONObject temp=(JSONObject)platform_list.get(i);
                collection = gridStore.getCollection("Platform");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("locationId"));
                row.setValue(3, temp.get("ownerId"));
                row.setValue(4, temp.get("type"));
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

    public void addPresenceAndOccupancyData() throws GSException {

        Collection<String, Row> collection;
        Row row;

        try {
            ClassLoader classLoader = getClass().getClassLoader();
            jsonObject = (JSONObject) parser.parse(new InputStreamReader(
                    classLoader.getResourceAsStream("Presence_dummy_data")));

            // Adding Presence
            JSONArray presence_list = (JSONArray) jsonObject.get("Presence");

            for(int i =0;i<presence_list.size();i++){
                JSONObject temp=(JSONObject)presence_list.get(i);
                collection = gridStore.getCollection("Presence");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("locationid"));
                row.setValue(2, temp.get("userid"));
                row.setValue(3, temp.get("timestamp"));
                collection.put(row);
            }

            // Adding Occupancy
            jsonObject = (JSONObject) parser.parse(new InputStreamReader(
                    classLoader.getResourceAsStream("Occupancy_dummy_data")));

            // Adding Presence
            JSONArray occupancy_list = (JSONArray) jsonObject.get("Occupancy");

            for(int i =0;i<occupancy_list.size();i++){
                JSONObject temp=(JSONObject)occupancy_list.get(i);
                collection = gridStore.getCollection("Occupancy");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("infraid"));
                row.setValue(2, temp.get("occupancy"));
                row.setValue(3, temp.get("timestamp"));
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

    public void addVSAndSemanticObservations() throws GSException {

        Collection<String, Row> collection;
        Row row;

        // Adding SemanticObservationTypes
        collection = gridStore.getCollection("User");
        row = collection.createRow();
        row.setValue(0, "user1");
        row.setValue(1, "peeyushg@uci.edu");
        row.setValue(2, "Peeyush");
        row.setValue(3, new String[]{"group1"});

        collection.put(row);

        // Adding Virtual Sensor Types
        collection = gridStore.getCollection("User");
        row = collection.createRow();
        row.setValue(0, "user1");
        row.setValue(1, "peeyushg@uci.edu");
        row.setValue(2, "Peeyush");
        row.setValue(3, new String[]{"group1"});

        collection.put(row);

        // Adding Virtual Sensors
        collection = gridStore.getCollection("User");
        row = collection.createRow();
        row.setValue(0, "user1");
        row.setValue(1, "peeyushg@uci.edu");
        row.setValue(2, "Peeyush");
        row.setValue(3, new String[]{"group1"});

        collection.put(row);

        //  TODO: Adding dynamic semantic observations

    }

}

