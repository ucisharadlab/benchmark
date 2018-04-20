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

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PulsarDataUploader extends BaseDataUploader {

    private String schema;
    private String CREATE_SCHEMA_FORMAT = "pulsar/schema/mapping%s/create.sql";
    private String DROP_FORMAT = "pulsar/schema/mapping%s/drop.sql";
    private String PULSAR_TABLE_NAME = "TippersBigTable";
    private int counter = 1;
    private int NUM_COLUMNS = 67;

    private JSONParser parser = new JSONParser();
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    BufferedWriter writer = null;
    PulsarConnectionManager connectionManager = PulsarConnectionManager.getInstance();

    public PulsarDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
        schema = readSchema(mapping);

        try {
            writer = new BufferedWriter(new FileWriter(PulsarConnectionManager.getInsertFilePath()));
            //writer.write(String.format("INSERT INTO %s VALUES \n", PULSAR_TABLE_NAME));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeRow(List<String> row) throws BenchmarkException {
        try {
            writer.write(String.format("INSERT INTO %s VALUES \n", PULSAR_TABLE_NAME) +
                    " (" + row.stream().collect(Collectors.joining(",")) +");\n");
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Writing Pulsar inserts file");
        }
    }
    
    private String readSchema(int mapping) throws BenchmarkException {
        byte[] encoded = new byte[0];
        try {
            encoded = Files.readAllBytes(Paths.get(
                    PulsarDataUploader.class.getClassLoader().getResource(
                            String.format(CREATE_SCHEMA_FORMAT, mapping)).getPath()));
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
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        connectionManager.stopSDB();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //connectionManager.ingestFromCommandLine(schema);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        connectionManager.startSDB();
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

                writeRow(pulsarRow);
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

                writeRow(pulsarRow);
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

                writeRow(pulsarRow);
            }

            for (Object anInfra_list : infra_list) {
                JSONObject temp = (JSONObject) anInfra_list;
                JSONArray locations;
                locations = (JSONArray) temp.get("geometry");

                for (Object location : locations) {
                    pulsarRow = getNullRow("INFRASTRUCTURE_LOCATION");

                    pulsarRow.set(12, getQuotedString(temp.get("id")));
                    pulsarRow.set(13, getQuotedString(((JSONObject) location).get("id")));

                    writeRow(pulsarRow);
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

                writeRow(pulsarRow);
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

                writeRow(pulsarRow);

                groups = (JSONArray) temp.get("groups");
                for (Object group : groups) {
                    pulsarRow = getNullRow("USER_GROUP_MEMBERSHIP");

                    pulsarRow.set(21, getQuotedString( temp.get("id")));
                    pulsarRow.set(22, getQuotedString(((JSONObject) group).get("id")));

                    writeRow(pulsarRow);
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

                writeRow(pulsarRow);
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

                writeRow(pulsarRow);

                JSONArray entitiesCovered = (JSONArray) temp.get("coverage");
                for (Object anEntitiesCovered : entitiesCovered) {
                    pulsarRow = getNullRow("SENSOR_COVERAGE");

                    pulsarRow.set(35, getQuotedString( temp.get("id")));
                    pulsarRow.set(36, getQuotedString(((JSONObject) anEntitiesCovered).get("name")));

                    writeRow(pulsarRow);
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

                writeRow(pulsarRow);
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

                writeRow(pulsarRow);
            }

        } catch(ParseException | IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Adding Deices");
        }
    }

    @Override
    public void addObservationData() throws BenchmarkException {
        List<String> pulsarRow;
        BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
                Observation.class);
        Observation obs = null;
        while ((obs = reader.readNext()) != null) {
            pulsarRow = null;
            if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                pulsarRow = getNullRow("ThermometerObservation");
                pulsarRow.set(48, obs.getPayload().get("temperature").getAsString());
                pulsarRow.set(47, getQuotedString(obs.getSensor().getId()));
                pulsarRow.set(46, getQuotedString(sdf.format(obs.getTimeStamp())));
                pulsarRow.set(45, getQuotedString(obs.getId()));

            } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                pulsarRow = getNullRow("WiFiAPObservation");
                pulsarRow.set(49, getQuotedString(obs.getPayload().get("clientId").getAsString()));
                pulsarRow.set(47, getQuotedString(obs.getSensor().getId()));
                pulsarRow.set(46, getQuotedString(sdf.format(obs.getTimeStamp())));
                pulsarRow.set(45, getQuotedString(obs.getId()));

            } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                pulsarRow = getNullRow("WeMoObservation");
                pulsarRow.set(50, obs.getPayload().get("currentMilliWatts").getAsString());
                pulsarRow.set(51, obs.getPayload().get("onTodaySeconds").getAsString());
                pulsarRow.set(47, getQuotedString(obs.getSensor().getId()));
                pulsarRow.set(46, getQuotedString(sdf.format(obs.getTimeStamp())));
                pulsarRow.set(45, getQuotedString(obs.getId()));
            }
            writeRow(pulsarRow);
        }
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
