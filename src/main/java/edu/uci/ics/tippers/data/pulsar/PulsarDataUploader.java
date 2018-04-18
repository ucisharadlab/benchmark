package edu.uci.ics.tippers.data.pulsar;

import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.connection.pulsar.PulsarConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.model.observation.Observation;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class PulsarDataUploader extends BaseDataUploader {

    private String schema;
    private List<List<String>> inserts;
    private String CREATE_SCHEMA_FORMAT = "pulsar/schema/mapping%s/create.sql";
    private String DROP_FORMAT = "postgresql/schema/mapping%s/drop.sql";
    private String PULSAR_TABLE_NAME = "TippersBigTable";
    private int counter = 1;
    private int NUM_COLUMNS = 67;

    private JSONParser parser = new JSONParser();


    public PulsarDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
        inserts = new ArrayList<>();
        schema = readSchema(mapping);
        addAllData();
        PulsarConnectionManager connectionManager = PulsarConnectionManager.getInstance();
        connectionManager.ingest(schema, PULSAR_TABLE_NAME, inserts);
    }

    private String readSchema(int mapping) throws BenchmarkException {
        byte[] encoded = new byte[0];
        try {
            encoded = Files.readAllBytes(Paths.get(String.format(CREATE_SCHEMA_FORMAT, mapping)));
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Reading Pulsar Schema File");
        }
        return new String(encoded, Charset.defaultCharset());
    }

    private List<String> getNullRow(String rowType){
        List<String> nullRow = new ArrayList<>();
        for (int i=0; i<NUM_COLUMNS; i++) {
            nullRow.add("NULL");
        }
        nullRow.set(0, String.valueOf(counter));
        nullRow.set(NUM_COLUMNS-1, getQuotedString(rowType));
        counter += 1;
        return nullRow;
    }

    private String getQuotedString(Object str){
        return "'" + str + "'";
    }

    @Override
    public Database getDatabase() {
        return Database.PULSAR;
    }

    @Override
    public Duration addAllData() throws BenchmarkException {
        Instant start = Instant.now();
        addUserData();
        addInfrastructureData();
        addDeviceData();
        addSensorData();
        addObservationData(); //no sensitivity
        Instant end = Instant.now();
        return Duration.between(start, end);
    }

    @Override
    public void addInfrastructureData() throws BenchmarkException {
        try {
            List<String> pulsarRow;

            // Adding Locations
            JSONArray location_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.LOCATION.getPath())));
            for (Object aLocation_list : location_list) {
                pulsarRow = getNullRow("LOCATION");
                JSONObject temp = (JSONObject) aLocation_list;

                pulsarRow.set(1, getQuotedString(temp.get("id")));
                pulsarRow.set(2, temp.get("x").toString());
                pulsarRow.set(3, temp.get("y").toString());
                pulsarRow.set(4, temp.get("z").toString());

                inserts.add(pulsarRow);
            }

            // Adding Infrastructure Type
            JSONArray infraType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA_TYPE.getPath())));
            for (Object anInfraType_list : infraType_list) {
                JSONObject temp = (JSONObject) anInfraType_list;
                pulsarRow = getNullRow("INFRASTRUCTURE_TYPE");

                pulsarRow.set(5, getQuotedString(temp.get("id")));
                pulsarRow.set(6, getQuotedString(temp.get("name")));
                pulsarRow.set(7, getQuotedString(temp.get("description")));

                inserts.add(pulsarRow);
            }

            // Adding Infrastructure
            JSONArray infra_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA.getPath())));
            for (Object anInfra_list : infra_list) {
                JSONObject temp = (JSONObject) anInfra_list;
                pulsarRow = getNullRow("INFRASTRUCTURE");

                pulsarRow.set(8, getQuotedString(temp.get("id")));
                pulsarRow.set(9, getQuotedString(temp.get("name")));
                pulsarRow.set(10, getQuotedString(((JSONObject) temp.get("type_")).get("id")));
                pulsarRow.set(11, temp.get("floor").toString());

                inserts.add(pulsarRow);
            }

            for (Object anInfra_list : infra_list) {
                JSONObject temp = (JSONObject) anInfra_list;
                JSONArray locations;
                locations = (JSONArray) temp.get("geometry");

                for (Object location : locations) {
                    pulsarRow = getNullRow("INFRASTRUCTURE_LOCATION");

                    pulsarRow.set(12, getQuotedString(temp.get("id")));
                    pulsarRow.set(13, getQuotedString(((JSONObject) location).get("id")));

                    inserts.add(pulsarRow);
                }
            }

        } catch(ParseException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addUserData() throws BenchmarkException {
        try {
            List<String> pulsarRow;

            // Adding Groups
            JSONArray group_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.GROUP.getPath())));
            for (Object aGroup_list : group_list) {
                JSONObject temp = (JSONObject) aGroup_list;
                pulsarRow = getNullRow("USER_GROUP");

                pulsarRow.set(14, getQuotedString(temp.get("id")));
                pulsarRow.set(15, getQuotedString(temp.get("name")));
                pulsarRow.set(16, getQuotedString(temp.get("description")));

                inserts.add(pulsarRow);
            }

            // Adding Users
            JSONArray user_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.USER.getPath())));
            for (Object anUser_list : user_list) {

                JSONObject temp = (JSONObject) anUser_list;
                JSONArray groups;
                pulsarRow = getNullRow("USER");

                pulsarRow.set(17, getQuotedString(temp.get("id")));
                pulsarRow.set(18, getQuotedString(temp.get("emailId")));
                pulsarRow.set(19, getQuotedString( temp.get("name")));
                pulsarRow.set(20, getQuotedString( temp.get("googleAuthToken")));

                inserts.add(pulsarRow);

                groups = (JSONArray) temp.get("groups");
                for (Object group : groups) {
                    pulsarRow = getNullRow("USER_GROUP_MEMBERSHIP");

                    pulsarRow.set(21, getQuotedString( temp.get("id")));
                    pulsarRow.set(22, getQuotedString(((JSONObject) group).get("id")));

                    inserts.add(pulsarRow);
                }
            }

        } catch(ParseException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addSensorData() throws BenchmarkException {
        try {
            List<String> pulsarRow;
            // Adding Sensor Types
            JSONArray sensorType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR_TYPE.getPath())));
            for (Object aSensorType_list : sensorType_list) {
                JSONObject temp = (JSONObject) aSensorType_list;
                pulsarRow = getNullRow("SENSOR_TYPE");

                pulsarRow.set(23, getQuotedString( temp.get("id")));
                pulsarRow.set(24, getQuotedString( temp.get("name")));
                pulsarRow.set(25, getQuotedString( temp.get("description")));
                pulsarRow.set(26, getQuotedString( temp.get("mobility")));
                pulsarRow.set(27, getQuotedString( temp.get("captureFunctionality")));
                pulsarRow.set(28, getQuotedString( temp.get("payloadSchema")));

                inserts.add(pulsarRow);
            }


            // Adding Sensors

            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR.getPath())));
            for (Object aSensor_list : sensor_list) {
                JSONObject temp = (JSONObject) aSensor_list;
                pulsarRow = getNullRow("SENSOR");

                pulsarRow.set(29, getQuotedString( temp.get("id")));
                pulsarRow.set(30, getQuotedString( temp.get("name")));
                pulsarRow.set(31, getQuotedString( ((JSONObject) temp.get("infrastructure")).get("id")));
                pulsarRow.set(32, getQuotedString( ((JSONObject) temp.get("owner")).get("id")));
                pulsarRow.set(33, getQuotedString( ((JSONObject) temp.get("type_")).get("id")));
                pulsarRow.set(34, getQuotedString( temp.get("sensorConfig")));

                inserts.add(pulsarRow);

                JSONArray entitiesCovered = (JSONArray) temp.get("coverage");
                for (Object anEntitiesCovered : entitiesCovered) {
                    pulsarRow = getNullRow("SENSOR_COVERAGE");

                    pulsarRow.set(35, getQuotedString( temp.get("id")));
                    pulsarRow.set(36, getQuotedString(((JSONObject) anEntitiesCovered).get("name")));

                    inserts.add(pulsarRow);
                }
            }

        } catch(ParseException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addDeviceData() throws BenchmarkException {
        try {
            List<String> pulsarRow;
            // Adding PlatformTypes
            JSONArray platformType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT_TYPE.getPath())));
            for (Object aPlatformType_list : platformType_list) {
                JSONObject temp = (JSONObject) aPlatformType_list;
                pulsarRow = getNullRow("PLATFORM_TYPE");

                pulsarRow.set(37, getQuotedString( temp.get("id")));
                pulsarRow.set(38, getQuotedString( temp.get("name")));
                pulsarRow.set(39, getQuotedString( temp.get("description")));

                inserts.add(pulsarRow);
            }

            // Adding Platforms
            JSONArray platform_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT.getPath())));
            for (Object aPlatform_list : platform_list) {
                JSONObject temp = (JSONObject) aPlatform_list;
                pulsarRow = getNullRow("PLATFORM");

                pulsarRow.set(40, getQuotedString( temp.get("id")));
                pulsarRow.set(41, getQuotedString( temp.get("name")));
                pulsarRow.set(42, getQuotedString( ((JSONObject)temp.get("owner")).get("id")));
                pulsarRow.set(43, getQuotedString( ((JSONObject)temp.get("type_")).get("id")));
                pulsarRow.set(44, getQuotedString( temp.get("hashedMac")));

                inserts.add(pulsarRow);
            }

        } catch(ParseException | IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Adding Deices");
        }
    }

    @Override
    public void addObservationData() throws BenchmarkException {
//        List<String> pulsarRow;
//        BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
//                Observation.class);
//        Observation obs = null;
//
//        JSONArray wifiArray = new JSONArray();
//        JSONArray wemoArray = new JSONArray();
//        JSONArray temArray = new JSONArray();
//
//        int wifiCount = 1, wemoCount = 1, thermoCount = 1, count = 0;
//        while ((obs = reader.readNext()) != null) {
//
//            JSONObject observationRow = new JSONObject();
//            if (obs.getSensor().getType_().getId().equals("Thermometer")) {
//                observationRow.put("temperature", obs.getPayload().get("temperature").getAsString());
//                observationRow.put("sensor_id", obs.getSensor().getId());
//                observationRow.put("timeStamp", String.valueOf(obs.getTimeStamp().getTime()/1000));
//                observationRow.put("id", obs.getId());
//                temArray.add(observationRow);
//                thermoCount ++;
//            } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
//                observationRow.put("clientId", obs.getPayload().get("clientId").getAsString());
//                observationRow.put("sensor_id", obs.getSensor().getId());
//                observationRow.put("timeStamp", String.valueOf(obs.getTimeStamp().getTime()/1000));
//                observationRow.put("id", obs.getId());
//                wifiArray.add(observationRow);
//                wifiCount ++;
//            } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
//                observationRow.put("currentMilliWatts", obs.getPayload().get("currentMilliWatts").getAsString());
//                observationRow.put("onTodaySeconds", obs.getPayload().get("onTodaySeconds").getAsString());
//                observationRow.put("sensor_id", obs.getSensor().getId());
//                observationRow.put("timeStamp", String.valueOf(obs.getTimeStamp().getTime()/1000));
//                observationRow.put("id", obs.getId());
//                wemoArray.add(observationRow);
//                wemoCount ++;
//            }
//
//            if (wemoCount % Constants.JANA_BATCH_SIZE == 0) {
//                connectionManager.doInsert("WeMoObservation", wemoArray);
//                wemoArray = new JSONArray();
//            }
//            if (wifiCount % Constants.JANA_BATCH_SIZE == 0) {
//                connectionManager.doInsert("WiFiAPObservation", wifiArray);
//                wifiArray = new JSONArray();
//            }
//            if (thermoCount % Constants.JANA_BATCH_SIZE == 0) {
//                connectionManager.doInsert("ThermometerObservation", temArray);
//                temArray = new JSONArray();
//            }
//            if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
//            count ++;
//        }
//        connectionManager.doInsert("WeMoObservation", wemoArray);
//        connectionManager.doInsert("WiFiAPObservation", wifiArray);
//        connectionManager.doInsert("ThermometerObservation", temArray);

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
