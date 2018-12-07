package edu.uci.ics.tippers.data.griddb.mappings;

import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.data.griddb.GridDBBaseDataMapping;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.model.observation.Observation;
import edu.uci.ics.tippers.model.semanticObservation.SemanticObservation;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GridDBDataMapping1 extends GridDBBaseDataMapping {

    private static final Logger LOGGER = Logger.getLogger(GridDBDataMapping1.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat qsdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private JSONParser parser = new JSONParser();

    public GridDBDataMapping1(GridStore gridStore, String dataDir) {
        super(gridStore, dataDir);
        //sdf.setTimeZone(TimeZone.getTimeZone("PDT"));
    }

    public void addAll() throws GSException {
        addMetadata();
        addDevices();
        addVSAndSemanticObservations();
        addSensorsAndObservations();
    }

    private List<Row> runQueryWithRows(String containerName, String query) throws BenchmarkException {
        try {
            Container<String, Row> container = gridStore.getContainer(containerName);
            Query<Row> gridDBQuery = container.query(query);
            RowSet<Row> rows = gridDBQuery.fetch();

            List<Row> rowList = new ArrayList<>();
            while (rows.hasNext()) {
                rowList.add(rows.next());
            }
            return rowList;
        } catch (GSException ge) {
            ge.printStackTrace();
            throw new BenchmarkException("Error Running Query On GridDB");
        }
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
//            BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
//                    Observation.class);
//            Observation obs = null;
//            int count = 0;
//
//            while ((obs = reader.readNext()) != null) {
//                String collectionName = Constants.GRIDDB_OBS_PREFIX + obs.getSensor().getId();
//
//                TimeSeries<Row> timeSeries = gridStore.getTimeSeries(collectionName);
//
//                row = timeSeries.createRow();
//                row.setValue(0, obs.getTimeStamp());
//                row.setValue(1, obs.getId());
//
//
//
//                if (obs.getSensor().getType_().getId().equals("Thermometer")) {
//                    row.setValue(2, obs.getPayload().get("temperature").getAsInt());
//                } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
//                    String query = String.format("SELECT * FROM %s WHERE timeStamp=TIMESTAMP('%s')",
//                            collectionName, qsdf.format(obs.getTimeStamp()));
//                    List<Row> rowAdded = runQueryWithRows(collectionName, query);
//                    if (rowAdded == null || rowAdded.isEmpty()) {
//                        row.setValue(2, new String[]{obs.getPayload().get("clientId").getAsString()});
//                    } else {
//                        String[] arr = rowAdded.get(0).getStringArray(2);
//                        arr = Arrays.copyOf(arr, arr.length +1);
//                        arr[arr.length-1] = obs.getPayload().get("clientId").getAsString();
//                        row.setValue(2, arr);
//                    }
//                } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
//                    row.setValue(2, obs.getPayload().get("currentMilliWatts").getAsInt());
//                    row.setValue(3, obs.getPayload().get("onTodaySeconds").getAsInt());
//                }
//                timeSeries.put(row);
//                if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
//                count ++;
//            }

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

    public void addVSAndSemanticObservations() throws GSException {

        Collection<String, Row> collection;
        Row row;
        try {

            // Adding Semantic Observation Types
            JSONArray soType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SO_TYPE.getPath())));

            for(int i =0;i<soType_list.size();i++){
                JSONObject temp=(JSONObject)soType_list.get(i);
                collection = gridStore.getCollection("SemanticObservationType");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("description"));
                collection.put(row);
            }

            // Adding Virtual Sensor Types
            JSONArray vsensorType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.VS_TYPE.getPath())));

            for(int i =0;i<vsensorType_list.size();i++){
                JSONObject temp=(JSONObject)vsensorType_list.get(i);
                collection = gridStore.getCollection("VirtualSensorType");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("description"));
                row.setValue(3, ((JSONObject)temp.get("inputType")).get("id"));
                row.setValue(4, ((JSONObject)temp.get("semanticObservationType")).get("id"));

                collection.put(row);
            }

            // Adding Virtual Sensors
            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.VS.getPath())));

            for(int i =0;i<sensor_list.size();i++){
                JSONObject temp=(JSONObject)sensor_list.get(i);
                collection = gridStore.getCollection("VirtualSensor");

                row = collection.createRow();
                row.setValue(0, temp.get("id"));
                row.setValue(1, temp.get("name"));
                row.setValue(2, temp.get("description"));
                row.setValue(3, temp.get("language"));
                row.setValue(4, temp.get("projectName"));
                row.setValue(5, ((JSONObject)temp.get("type_")).get("id"));

                collection.put(row);

            }

            // Adding Semantic Observations
//            BigJsonReader<SemanticObservation> reader = new BigJsonReader<>(dataDir + DataFiles.SO.getPath(),
//                    SemanticObservation.class);
//            SemanticObservation sobs = null;
//            int count = 0;
//
//            while ((sobs = reader.readNext()) != null) {
//                String collectionName = Constants.GRIDDB_SO_PREFIX + sobs.getSemanticEntity().get("id").getAsString();
//
//                TimeSeries<Row> timeSeries = gridStore.getTimeSeries(collectionName);
//
//                row = timeSeries.createRow();
//                row.setValue(1, sobs.getId());
//                row.setValue(0, sobs.getTimeStamp());
//                row.setValue(2, sobs.getVirtualSensor().getId());
//
//                if (sobs.getType_().getId().equals("occupancy")) {
//                    row.setValue(3, sobs.getPayload().get("occupancy").getAsInt());
//                } else if (sobs.getType_().getId().equals("presence")) {
//                    row.setValue(3, sobs.getPayload().get("location").getAsString());
//                }
//                timeSeries.put(row);
//                if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s S Observations", count));
//                count ++;
//            }

        }
        catch(ParseException | IOException e) {
            e.printStackTrace();
        }

    }

}

