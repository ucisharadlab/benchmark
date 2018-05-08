package edu.uci.ics.tippers.data.influxdb;

import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.connection.influxdb.InfluxDBConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.model.observation.Observation;
import edu.uci.ics.tippers.model.semanticObservation.SemanticObservation;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBDataUploader extends BaseDataUploader {

    private Connection metadataConnection;
    private static final Logger LOGGER = Logger.getLogger(InfluxDBDataUploader.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private JSONParser parser = new JSONParser();
    
    public InfluxDBDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
        metadataConnection = InfluxDBConnectionManager.getInstance().getMetadataConnection();

    }

    @Override
    public Database getDatabase() {
        return Database.INFLUXDB;
    }

    @Override
    public Duration addAllData() throws BenchmarkException {
        Instant start = Instant.now();
        addMetadata();
        addDevices();
        addSensors();
        addVirtualSensors();
        addObservationData();
        addSemanticObservationData();
        Instant end = Instant.now();
        return Duration.between(start, end);
    }

    public void addMetadata() throws BenchmarkException {

        PreparedStatement stmt;
        String insert;

        try {

            // Adding Groups
            insert ="INSERT INTO USER_GROUP " + "(ID, NAME, DESCRIPTION) VALUES (?, ?, ?)";
            JSONArray group_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.GROUP.getPath())));
            stmt = metadataConnection.prepareStatement(insert);

            for(int i =0;i<group_list.size();i++){
                JSONObject temp=(JSONObject)group_list.get(i);
                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(3, (String)temp.get("description"));
                stmt.executeUpdate();
            }

            // Adding Users
            insert = "INSERT INTO USERS " + "(ID, EMAIL, GOOGLE_AUTH_TOKEN, NAME) VALUES (?, ?, ?, ?)";
            JSONArray user_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.USER.getPath())));
            stmt = metadataConnection.prepareStatement(insert);

            String membership = "INSERT INTO USER_GROUP_MEMBERSHIP " + "(USER_ID, USER_GROUP_ID) VALUES (?, ?)";
            PreparedStatement memStmt = metadataConnection.prepareStatement(membership);

            for(int i =0;i<user_list.size();i++){

                JSONObject temp=(JSONObject)user_list.get(i);
                JSONArray groups;

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("emailId"));
                stmt.setString(4, (String)temp.get("name"));
                stmt.setString(3, (String)temp.get("googleAuthToken"));

                stmt.executeUpdate();


                groups=(JSONArray)temp.get("groups");
                memStmt.setString(1, (String)temp.get("id"));
                for(int x=0;x<groups.size();x++) {
                    memStmt.setString(2, ((JSONObject)groups.get(x)).get("id").toString());
                    memStmt.executeUpdate();
                }
            }

            // Adding Locations
            insert = "INSERT INTO LOCATION " +
                    "(ID, X, Y, Z) VALUES (?, ?, ?, ?)";
            JSONArray location_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.LOCATION.getPath())));

            stmt = metadataConnection.prepareStatement(insert);
            for(int i =0;i<location_list.size();i++){
                JSONObject temp=(JSONObject)location_list.get(i);
                stmt.setString(1, (String)temp.get("id"));
                stmt.setDouble(2, ((Number)temp.get("x")).doubleValue());
                stmt.setDouble(3, ((Number)temp.get("y")).doubleValue());
                stmt.setDouble(4, ((Number)temp.get("z")).doubleValue());
                stmt.executeUpdate();

            }

            // Adding Infrastructure Type
            insert = "INSERT INTO INFRASTRUCTURE_TYPE " +
                    "(ID, NAME, DESCRIPTION) VALUES (?, ?, ?)";
            JSONArray infraType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA_TYPE.getPath())));

            stmt = metadataConnection.prepareStatement(insert);
            for(int i =0;i<infraType_list.size();i++){
                JSONObject temp=(JSONObject)infraType_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(3, (String)temp.get("description"));

                stmt.executeUpdate();
            }

            // Adding Infrastructure
            insert = "INSERT INTO INFRASTRUCTURE " +
                    "(ID, INFRASTRUCTURE_TYPE_ID, NAME, FLOOR) VALUES (?, ?, ?, ?)";

            JSONArray infra_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA.getPath())));

            stmt = metadataConnection.prepareStatement(insert);
            for(int i =0;i<infra_list.size();i++){
                JSONObject temp=(JSONObject)infra_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(3, (String)temp.get("name"));
                stmt.setString(2, (String)((JSONObject)temp.get("type_")).get("id"));
                stmt.setInt(4, ((Number)temp.get("floor")).intValue());
                stmt.executeUpdate();
            }

            String regionInfra = "INSERT INTO INFRASTRUCTURE_LOCATION " + "(LOCATION_ID, INFRASTRUCTURE_ID) VALUES (?, ?)";
            PreparedStatement regionInfraStmt = metadataConnection.prepareStatement(regionInfra);


            for(int i =0;i<infra_list.size();i++){
                JSONObject temp=(JSONObject)infra_list.get(i);
                JSONArray locations;

                locations = (JSONArray)temp.get("geometry");
                regionInfraStmt.setString(2, (String)temp.get("id"));
                for(int x=0; x<locations.size(); x++) {
                    regionInfraStmt.setString(1, ((JSONObject) locations.get(x)).get("id").toString());
                }
                regionInfraStmt.executeUpdate();
            }

        }
        catch(ParseException | SQLException | IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Uploading Data");
        }

    }

    public void addSensors() {

        PreparedStatement stmt;
        String insert;

        try {
            // Adding Sensor Types
            insert = "INSERT INTO SENSOR_TYPE " +
                    "(ID, NAME, DESCRIPTION, MOBILITY, CAPTURE_FUNCTIONALITY, PAYLOAD_SCHEMA) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
            JSONArray sensorType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR_TYPE.getPath())));
            stmt = metadataConnection.prepareStatement(insert);
            for(int i =0;i<sensorType_list.size();i++){
                JSONObject temp=(JSONObject)sensorType_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(3, (String)temp.get("description"));
                stmt.setString(4, (String)temp.get("mobility"));
                stmt.setString(5, (String)temp.get("captureFunctionality"));
                stmt.setString(6, (String)temp.get("payloadSchema"));
                stmt.executeUpdate();

            }



            // Adding Sensors
            insert = "INSERT INTO SENSOR" +
                    "(ID, NAME, INFRASTRUCTURE_ID, USER_ID, SENSOR_TYPE_ID, SENSOR_CONFIG) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";


            String covInfrastructure = "INSERT INTO Coverage_Infrastructure" +
                    "(SENSOR_ID, INFRASTRUCTURE_ID) " +
                    "VALUES (?, ?)" ;

            PreparedStatement covInfraStmt = metadataConnection.prepareStatement(covInfrastructure);

            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR.getPath())));

            stmt = metadataConnection.prepareStatement(insert);
            for(int i =0;i<sensor_list.size();i++){
                JSONObject temp=(JSONObject)sensor_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(3, (String)((JSONObject)temp.get("infrastructure")).get("id"));
                stmt.setString(4, (String)((JSONObject)temp.get("owner")).get("id"));
                stmt.setString(5, (String)((JSONObject)temp.get("type_")).get("id"));
                stmt.setString(6, (String)temp.get("sensorConfig"));
                stmt.executeUpdate();

                JSONArray entitiesCovered = (JSONArray) temp.get("coverage");
                covInfraStmt.setString(1, (String)temp.get("id"));
                for(int x=0; x<entitiesCovered.size(); x++) {
                    covInfraStmt.setString(2,((JSONObject)entitiesCovered.get(x)).get("name").toString());
                    covInfraStmt.executeUpdate();
                }

            }

        }
        catch(ParseException | SQLException | IOException e) {
            e.printStackTrace();
        }

    }

    public void addDevices() {

        PreparedStatement stmt;
        String insert;

        try {
            // Adding PlatformTypes
            insert = "INSERT INTO PLATFORM_TYPE" + "(ID, NAME, DESCRIPTION) VALUES (?, ?, ?)";
            JSONArray platformType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT_TYPE.getPath())));

            for(int i =0;i<platformType_list.size();i++){
                stmt = metadataConnection.prepareStatement(insert);

                JSONObject temp=(JSONObject)platformType_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(3, (String)temp.get("description"));
                stmt.executeUpdate();

            }

            // Adding Platforms
            insert = "INSERT INTO PLATFORM " + "(ID, NAME, " +
                    "USER_ID, PLATFORM_TYPE_ID, HASHED_MAC) VALUES (?, ?, ?, ?, ?)";
            JSONArray platform_list = (JSONArray)parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT.getPath())));

            for(int i =0;i<platform_list.size();i++){
                stmt = metadataConnection.prepareStatement(insert);
                JSONObject temp=(JSONObject)platform_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(3, (String) ((JSONObject)temp.get("owner")).get("id"));
                stmt.setString(4, (String) ((JSONObject)temp.get("type_")).get("id"));
                stmt.setString(5, (String)temp.get("hashedMac"));
                stmt.executeUpdate();

            }

        }
        catch(ParseException | SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    public void addVirtualSensors() {

        PreparedStatement stmt;
        String insert;
        try {

            // Adding Semantic Observation Types
            insert = "INSERT INTO SEMANTIC_OBSERVATION_TYPE" + "(ID, NAME, DESCRIPTION) VALUES (?, ?, ?)";
            JSONArray soType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SO_TYPE.getPath())));

            stmt = metadataConnection.prepareStatement(insert);
            for(int i =0;i<soType_list.size();i++){
                JSONObject temp=(JSONObject)soType_list.get(i);

                stmt.setString(1, temp.get("id").toString());
                stmt.setString(2, temp.get("name").toString());
                stmt.setString(3, temp.get("description").toString());
                stmt.executeUpdate();
            }

            // Adding Virtual Sensor Types
            insert = "INSERT INTO VIRTUAL_SENSOR_TYPE" + "(ID, NAME, DESCRIPTION, INPUT_TYPE_ID, " +
                    "SEMANTIC_OBSERVATION_TYPE_ID) VALUES (?, ?, ?, ?, ?)";
            JSONArray vsensorType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.VS_TYPE.getPath())));

            stmt = metadataConnection.prepareStatement(insert);
            for(int i =0;i<vsensorType_list.size();i++){
                JSONObject temp=(JSONObject)vsensorType_list.get(i);

                stmt.setString(1, temp.get("id").toString());
                stmt.setString(2, temp.get("name").toString());
                stmt.setString(3, temp.get("description").toString());
                stmt.setString(4, ((JSONObject)temp.get("inputType")).get("id").toString());
                stmt.setString(5, ((JSONObject)temp.get("semanticObservationType")).get("id").toString());

                stmt.executeUpdate();
            }

            // Adding Virtual Sensors
            insert = "INSERT INTO VIRTUAL_SENSOR" + "(ID, NAME, DESCRIPTION, LANGUAGE, PROJECT_NAME, TYPE_ID) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.VS.getPath())));

            stmt = metadataConnection.prepareStatement(insert);
            for(int i =0;i<sensor_list.size();i++){
                JSONObject temp=(JSONObject)sensor_list.get(i);

                stmt.setString(1, temp.get("id").toString());
                stmt.setString(2, temp.get("name").toString());
                stmt.setString(3, temp.get("description").toString());
                stmt.setString(4, temp.get("language").toString());
                stmt.setString(5, temp.get("projectName").toString());
                stmt.setString(6, ((JSONObject)temp.get("type_")).get("id").toString());

                stmt.executeUpdate();
            }

        }
        catch(ParseException | IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void addObservationData() throws BenchmarkException {

        BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
                Observation.class);
        Observation obs = null;

        int count = 0;
        List<String> measurements = new ArrayList<>();
        while ((obs = reader.readNext()) != null) {

            if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                measurements.add(String.format("Thermometer,sensorId=%s id=%s,temperature=%s %s",
                        obs.getSensor().getId(), obs.getId(), obs.getPayload().get("temperature").getAsInt(),
                        sdf.format(obs.getTimeStamp().getTime())));

            } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                measurements.add(String.format("WiFiAP,sensorId=%s id=%s,clientId=%s %s",
                        obs.getSensor().getId(), obs.getId(), obs.getPayload().get("clientId").getAsString(),
                        sdf.format(obs.getTimeStamp().getTime())));

            } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                measurements.add(String.format("WeMo,sensorId=%s id=%s,currentMilliWatts=%s,onTodaySeconds=%s  %s",
                        obs.getSensor().getId(), obs.getId(),
                        obs.getPayload().get("currentMilliWatts").getAsInt(),
                        obs.getPayload().get("onTodaySeconds").getAsInt(),
                        sdf.format(obs.getTimeStamp().getTime())));
            }

            if (count % Constants.INFLUXDB_BATCH_SIZE == 0) {
                InfluxDBConnectionManager.getInstance().write(
                        measurements.stream().collect(Collectors.joining("\n")));
            }

            if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
            count ++;

        }
        InfluxDBConnectionManager.getInstance().write(measurements.stream().collect(Collectors.joining("\n")));
    }

    @Override
    public void addSemanticObservationData() {

        BigJsonReader<SemanticObservation> reader = new BigJsonReader<>(dataDir + DataFiles.SO.getPath(),
                SemanticObservation.class);
        SemanticObservation sobs = null;

        List<String> measurements = new ArrayList<>();
        int count = 0;
        while ((sobs = reader.readNext()) != null) {

            if (sobs.getType_().getId().equals("presence")) {
                measurements.add(String.format("presence,virtualSensorId=%s,semanticEntityId=%s id=%s,location=%s %s",
                        sobs.getVirtualSensor().getId(), sobs.getSemanticEntity().get("id").getAsString(),
                        sobs.getId(), sobs.getPayload().get("location").getAsString(),
                        sdf.format(sobs.getTimeStamp().getTime())));

            } else if (sobs.getType_().getId().equals("occupancy")) {
                measurements.add(String.format("occupancy,virtualSensorId=%s,semanticEntityId=%s id=%s,occupancy=%s %s",
                        sobs.getVirtualSensor().getId(), sobs.getSemanticEntity().get("id").getAsString(),
                        sobs.getId(), sobs.getPayload().get("occupancy").getAsInt(),
                        sdf.format(sobs.getTimeStamp().getTime())));
            }

            if (count % Constants.INFLUXDB_BATCH_SIZE == 0) {
                InfluxDBConnectionManager.getInstance().write(
                        measurements.stream().collect(Collectors.joining("\n")));
            }

            if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s S Observations", count));
            count ++;
        }
        InfluxDBConnectionManager.getInstance().write(measurements.stream().collect(Collectors.joining("\n")));

    }

    @Override
    public Duration insertPerformance() throws BenchmarkException {
        Instant start = Instant.now();

        try {

            BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.INSERT_TEST.getPath(),
                    Observation.class);
            Observation obs = null;

            List<String> measurements = new ArrayList<>();
            int count = 0;
            while ((obs = reader.readNext()) != null) {

                if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                    measurements.add(String.format("Thermometer,sensorId=%s id=%s,temperature=%s %s",
                            obs.getSensor().getId(), obs.getId(), obs.getPayload().get("temperature").getAsInt(),
                            sdf.format(obs.getTimeStamp().getTime())));

                } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                    measurements.add(String.format("Thermometer,sensorId=%s id=%s,clientId=%s %s",
                            obs.getSensor().getId(), obs.getId(), obs.getPayload().get("clientId").getAsString(),
                            sdf.format(obs.getTimeStamp().getTime())));

                } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                    measurements.add(String.format("Thermometer,sensorId=%s id=%s,currentMilliWatts=%s,onTodaySeconds=%s  %s",
                            obs.getSensor().getId(), obs.getId(),
                            obs.getPayload().get("currentMilliWatts").getAsInt(),
                            obs.getPayload().get("onTodaySeconds").getAsInt(),
                            sdf.format(obs.getTimeStamp().getTime())));
                }

                if (count % Constants.INFLUXDB_BATCH_SIZE == 0) {
                    InfluxDBConnectionManager.getInstance().write(
                            measurements.stream().collect(Collectors.joining("\n")));
                }

                if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
                count ++;

            }
            InfluxDBConnectionManager.getInstance().write(measurements.stream().collect(Collectors.joining("\n")));

        }
        catch(Exception e) {
            e.printStackTrace();
        }
        Instant end = Instant.now();
        return Duration.between(start, end);
    }

    @Override
    public void addInfrastructureData() throws BenchmarkException {

    }

    @Override
    public void addUserData() throws BenchmarkException {

    }

    @Override
    public void addSensorData() throws BenchmarkException {

    }

    @Override
    public void addDeviceData() throws BenchmarkException {

    }

    @Override
    public void virtualSensorData() {

    }
}
