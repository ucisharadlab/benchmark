package edu.uci.ics.tippers.data.jana;

import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.jana.JanaConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.exception.BenchmarkException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;

public class JanaDataUploader extends BaseDataUploader {

    private JanaConnectionManager connectionManager;
    private JSONParser parser = new JSONParser();

    public JanaDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
        connectionManager = JanaConnectionManager.getInstance();

    }

    @Override
    public Database getDatabase() {
        return Database.JANA;
    }

    @Override
    public Duration addAllData() throws BenchmarkException {
        Instant start = Instant.now();
        addUserData();
        addInfrastructureData();
        addDeviceData();
        addSensorData();
        //addObservationData();
        Instant end = Instant.now();
        return Duration.between(start, end);
    }

    @Override
    public void addInfrastructureData() throws BenchmarkException {
        try {
            // Adding Locations
            JSONArray location_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.LOCATION.getPath())));
            JSONArray locationArray = new JSONArray();

            for (Object aLocation_list : location_list) {
                JSONObject temp = (JSONObject) aLocation_list;
                JSONObject locationRow = new JSONObject();
                locationRow.put("ID", temp.get("id"));
                locationRow.put("X", temp.get("x").toString());
                locationRow.put("Y", temp.get("y").toString());
                locationRow.put("Z", temp.get("z").toString());

                locationArray.add(locationRow);
            }
            connectionManager.doInsert("LOCATION", locationArray);


            // Adding Infrastructure Type
            JSONArray infraType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA_TYPE.getPath())));

            JSONArray infraTypeArray = new JSONArray();
            for (Object anInfraType_list : infraType_list) {
                JSONObject temp = (JSONObject) anInfraType_list;
                JSONObject infraTypeRow = new JSONObject();

                infraTypeRow.put("ID", (String) temp.get("id"));
                infraTypeRow.put("NAME", (String) temp.get("name"));
                infraTypeRow.put("DESCRIPTION", (String) temp.get("description"));

                infraTypeArray.add(infraTypeRow);
            }
            connectionManager.doInsert("INFRASTRUCTURE_TYPE", infraTypeArray);

            // Adding Infrastructure
            JSONArray infra_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA.getPath())));
            JSONArray infraArray = new JSONArray();

            for (Object anInfra_list : infra_list) {
                JSONObject temp = (JSONObject) anInfra_list;
                JSONObject infraRow = new JSONObject();

                infraRow.put("ID", (String) temp.get("id"));
                infraRow.put("NAME", (String) temp.get("name"));
                infraRow.put("INFRASTRUCTURE_TYPE_ID", (String) ((JSONObject) temp.get("type_")).get("id"));
                infraRow.put("FLOOR", temp.get("floor").toString());

                infraArray.add(infraRow);
            }
            connectionManager.doInsert("INFRASTRUCTURE", infraArray);

            JSONArray infraLocationArray = new JSONArray();
            for (Object anInfra_list : infra_list) {
                JSONObject temp = (JSONObject) anInfra_list;
                JSONArray locations;

                locations = (JSONArray) temp.get("geometry");

                for (Object location : locations) {
                    JSONObject infraLocationRow = new JSONObject();

                    infraLocationRow.put("INFRASTRUCTURE_ID", (String) temp.get("id"));
                    infraLocationRow.put("LOCATION_ID", ((JSONObject) location).get("id").toString());

                    infraLocationArray.add(infraLocationRow);
                }
            }
            connectionManager.doInsert("INFRASTRUCTURE_LOCATION", infraLocationArray);

        } catch(ParseException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addUserData() throws BenchmarkException {

        try {
            // Adding Groups
            JSONArray group_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.GROUP.getPath())));
            JSONArray groupArray = new JSONArray();
            for (Object aGroup_list : group_list) {
                JSONObject temp = (JSONObject) aGroup_list;
                JSONObject groupRow = new JSONObject();

                groupRow.put("ID", (String) temp.get("id"));
                groupRow.put("NAME", (String) temp.get("name"));
                groupRow.put("DESCRIPTION", (String) temp.get("description"));

                groupArray.add(groupRow);
            }
            connectionManager.doInsert("USER_GROUP", groupArray);

            // Adding Users
            JSONArray user_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.USER.getPath())));
            JSONArray userArray = new JSONArray();
            JSONArray membershipArray = new JSONArray();

            for (Object anUser_list : user_list) {

                JSONObject temp = (JSONObject) anUser_list;
                JSONArray groups;
                JSONObject userRow = new JSONObject();

                userRow.put("ID", (String) temp.get("id"));
                userRow.put("EMAIL", (String) temp.get("emailId"));
                userRow.put("NAME", (String) temp.get("name"));
                userRow.put("GOOGLE_AUTH_TOKEN", (String) temp.get("googleAuthToken"));

                userArray.add(userRow);

                groups = (JSONArray) temp.get("groups");
                for (Object group : groups) {
                    JSONObject memberRow = new JSONObject();

                    memberRow.put("USER_ID", (String) temp.get("id"));
                    memberRow.put("USER_GROUP_ID", ((JSONObject) group).get("id").toString());

                    membershipArray.add(memberRow);
                }
            }
            connectionManager.doInsert("USERS", userArray);
            connectionManager.doInsert("USER_GROUP_MEMBERSHIP", membershipArray);

        } catch(ParseException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addSensorData() throws BenchmarkException {
        try {
            // Adding Sensor Types
            JSONArray sensorType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR_TYPE.getPath())));
            JSONArray sensorTypeArray =  new JSONArray();

            for (Object aSensorType_list : sensorType_list) {
                JSONObject temp = (JSONObject) aSensorType_list;
                JSONObject sensorTypeRow = new JSONObject();

                sensorTypeRow.put("ID", (String) temp.get("id"));
                sensorTypeRow.put("NAME", (String) temp.get("name"));
                sensorTypeRow.put("DESCRIPTION", (String) temp.get("description"));
                sensorTypeRow.put("MOBILITY", (String) temp.get("mobility"));
                sensorTypeRow.put("CAPTURE_FUNCTIONALITY", (String) temp.get("captureFunctionality"));
                sensorTypeRow.put("PAYLOAD_SCHEMA", (String) temp.get("payloadSchema"));

                sensorTypeArray.add(sensorTypeRow);
            }

            connectionManager.doInsert("SENSOR_TYPE", sensorTypeArray);


            // Adding Sensors

            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR.getPath())));
            JSONArray sensorArray = new JSONArray();
            JSONArray coverageArray = new JSONArray();

            for (Object aSensor_list : sensor_list) {
                JSONObject temp = (JSONObject) aSensor_list;
                JSONObject sensorRow = new JSONObject();

                sensorRow.put("ID", (String) temp.get("id"));
                sensorRow.put("NAME", (String) temp.get("name"));
                sensorRow.put("INFRASTRUCTURE_ID", (String) ((JSONObject) temp.get("infrastructure")).get("id"));
                sensorRow.put("USER_ID", (String) ((JSONObject) temp.get("owner")).get("id"));
                sensorRow.put("SENSOR_TYPE_ID", (String) ((JSONObject) temp.get("type_")).get("id"));
                sensorRow.put("SENSOR_CONFIG", (String) temp.get("sensorConfig"));

                sensorArray.add(sensorRow);

                JSONArray entitiesCovered = (JSONArray) temp.get("coverage");
                for (Object anEntitiesCovered : entitiesCovered) {
                    JSONObject coverageRow = new JSONObject();

                    coverageRow.put("SENSOR_ID", (String) temp.get("id"));
                    coverageRow.put("INFRASTRUCTURE_ID", ((JSONObject) anEntitiesCovered).get("name").toString());

                    coverageArray.add(coverageRow);
                }
            }
            connectionManager.doInsert("SENSOR", sensorArray);
            connectionManager.doInsert("COVERAGE_INFRASTRUCTURE", coverageArray);

        } catch(ParseException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addDeviceData() throws BenchmarkException {
        try {
            // Adding PlatformTypes
            JSONArray platformType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT_TYPE.getPath())));
            JSONArray platformTypeArray = new JSONArray();
            for (Object aPlatformType_list : platformType_list) {
                JSONObject temp = (JSONObject) aPlatformType_list;
                JSONObject platformTypeRow = new JSONObject();

                platformTypeRow.put("ID", (String) temp.get("id"));
                platformTypeRow.put("NAME", (String) temp.get("name"));
                platformTypeRow.put("DESCRIPTION", (String) temp.get("description"));

                platformTypeArray.add(platformTypeRow);
            }
            connectionManager.doInsert("PLATFORM_TYPE", platformTypeArray);

            // Adding Platforms
            JSONArray platform_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT.getPath())));
            JSONArray platformArray = new JSONArray();

            for (Object aPlatform_list : platform_list) {
                JSONObject temp = (JSONObject) aPlatform_list;
                JSONObject platformRow = new JSONObject();

                platformRow.put("ID", (String) temp.get("id"));
                platformRow.put("NAME", (String) temp.get("name"));
                platformRow.put("USER_ID", (String) ((JSONObject)temp.get("owner")).get("id"));
                platformRow.put("PLATFORM_TYPE_ID", (String) ((JSONObject)temp.get("type_")).get("id"));

                platformArray.add(platformRow);
            }
            connectionManager.doInsert("PLATFORM", platformArray);

        } catch(ParseException | IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Adding Deices");
        }

    }

    @Override
    public void addObservationData() throws BenchmarkException {

    }

    @Override
    public void virtualSensorData() {

    }

    @Override
    public void addSemanticObservationData() {

    }

    @Override
    public Duration insertPerformance() throws BenchmarkException {
        return null;
    }
}
