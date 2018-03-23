package edu.uci.ics.tippers.data.sparksql.mappings;

import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.data.sparksql.SparkSQLBaseDataMapping;
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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class SparkSQLDataMapping2 extends SparkSQLBaseDataMapping {

    private static final Logger LOGGER = Logger.getLogger(SparkSQLDataMapping2.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private JSONParser parser = new JSONParser();

    public SparkSQLDataMapping2(Connection connection, String dataDir) {
        super(connection, dataDir);
    }

    public void addAll() throws BenchmarkException{
        try {
            connection.setAutoCommit(false);
            addMetadata();
            addDevices();
            addSensorsAndObservations();
            addVSAndSemanticObservations();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addMetadata() throws BenchmarkException {

        PreparedStatement stmt;
        String insert;

        try {

            // Adding Groups
            insert ="INSERT INTO USER_GROUP " + " VALUES (?, ?, ?)";
            JSONArray group_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.GROUP.getPath())));
            stmt = connection.prepareStatement(insert);

            for(int i =0;i<group_list.size();i++){
                JSONObject temp=(JSONObject)group_list.get(i);
                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(3, (String)temp.get("name"));
                stmt.setString(2, (String)temp.get("description"));
                stmt.executeUpdate();
            }

            // Adding Users
            StringBuilder insertBuilder = new StringBuilder("INSERT INTO USERS " + " VALUES ");
            JSONArray user_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.USER.getPath())));
            stmt = connection.prepareStatement(insert);

            String membership = "INSERT INTO USER_GROUP_MEMBERSHIP " + " VALUES (?, ?)";
            PreparedStatement memStmt = connection.prepareStatement(membership);

            int count =0, batchSize = 1000;
            for(int i =0;i<user_list.size();i++){
                JSONObject temp=(JSONObject)user_list.get(i);
                JSONArray groups;

                StringBuilder parmasBuilder = new StringBuilder();
                parmasBuilder
                        .append("(")
                        .append((String)temp.get("id"))
                        .append(",")
                        .append((String)temp.get("emailId"))
                        .append(",")
                        .append((String)temp.get("name"))
                        .append(",")
                        .append((String)temp.get("googleAuthToken"))
                        .append(")");
                stmt.setString(4, (String)temp.get("id"));
                stmt.setString(1, (String)temp.get("emailId"));
                stmt.setString(3, (String)temp.get("name"));
                stmt.setString(2, (String)temp.get("googleAuthToken"));

                stmt.executeUpdate();

                groups=(JSONArray)temp.get("groups");
                memStmt.setString(1, (String)temp.get("id"));
                for(int x=0;x<groups.size();x++) {
                    memStmt.setString(2, ((JSONObject)groups.get(x)).get("id").toString());
                    memStmt.executeUpdate();
                }

                count ++;
                if (count % batchSize == 0) {
                    stmt.executeBatch();
                    memStmt.executeBatch();
                }

            }
//            stmt.executeBatch();
//            memStmt.executeBatch();

            // Adding Locations
            insert = "INSERT INTO LOCATION " +
                    " VALUES (?, ?, ?, ?)";
            JSONArray location_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.LOCATION.getPath())));

            stmt = connection.prepareStatement(insert);
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
                    " VALUES (?, ?, ?)";
            JSONArray infraType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA_TYPE.getPath())));

            stmt = connection.prepareStatement(insert);
            for(int i =0;i<infraType_list.size();i++){
                JSONObject temp=(JSONObject)infraType_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(3, (String)temp.get("name"));
                stmt.setString(2, (String)temp.get("description"));

                stmt.executeUpdate();
            }

            // Adding Infrastructure
            insert = "INSERT INTO INFRASTRUCTURE " +
                    " VALUES (?, ?, ?, ?)";

            JSONArray infra_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA.getPath())));

            stmt = connection.prepareStatement(insert);
            for(int i =0;i<infra_list.size();i++){
                JSONObject temp=(JSONObject)infra_list.get(i);

                stmt.setString(3, (String)temp.get("id"));
                stmt.setString(1, (String)temp.get("name"));
                stmt.setString(2, (String)((JSONObject)temp.get("type_")).get("id"));
                stmt.setInt(4, ((Number)temp.get("floor")).intValue());
                stmt.executeUpdate();
            }

            String regionInfra = "INSERT INTO INFRASTRUCTURE_LOCATION " + " VALUES (?, ?)";
            PreparedStatement regionInfraStmt = connection.prepareStatement(regionInfra);


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

    public void addSensorsAndObservations() {

        PreparedStatement stmt;
        String insert;

        try {
            // Adding Sensor Types
//            insert = "INSERT INTO SENSOR_TYPE " +
//                    " VALUES (?, ?, ?, ?, ?, ?)";
//            JSONArray sensorType_list = (JSONArray) parser.parse(new InputStreamReader(
//                    new FileInputStream(dataDir + DataFiles.SENSOR_TYPE.getPath())));
//            stmt = connection.prepareStatement(insert);
//            for(int i =0;i<sensorType_list.size();i++){
//                JSONObject temp=(JSONObject)sensorType_list.get(i);
//
//                stmt.setString(1, (String)temp.get("id"));
//                stmt.setString(4, (String)temp.get("name"));
//                stmt.setString(2, (String)temp.get("description"));
//                stmt.setString(3, (String)temp.get("mobility"));
//                stmt.setString(5, (String)temp.get("captureFunctionality"));
//                stmt.setString(6, (String)temp.get("payloadSchema"));
//                stmt.executeUpdate();
//
//            }
//
//
//
//            // Adding Sensors
//            insert = "INSERT INTO SENSOR " +
//                    " VALUES (?, ?, ?, ?, ?, ?)";
//
//
//            String covInfrastructure = "INSERT INTO Coverage_Infrastructure " +
//                    " VALUES (?, ?)" ;
//
//            PreparedStatement covInfraStmt = connection.prepareStatement(covInfrastructure);
//
//            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
//                    new FileInputStream(dataDir + DataFiles.SENSOR.getPath())));
//
//            stmt = connection.prepareStatement(insert);
//            for(int i =0;i<sensor_list.size();i++){
//                JSONObject temp=(JSONObject)sensor_list.get(i);
//
//                stmt.setString(1, (String)temp.get("id"));
//                stmt.setString(2, (String)temp.get("name"));
//                stmt.setString(3, (String)((JSONObject)temp.get("infrastructure")).get("id"));
//                stmt.setString(4, (String)((JSONObject)temp.get("owner")).get("id"));
//                stmt.setString(5, (String)((JSONObject)temp.get("type_")).get("id"));
//                stmt.setString(6, (String)temp.get("sensorConfig"));
//                stmt.executeUpdate();
//
//                JSONArray entitiesCovered = (JSONArray) temp.get("coverage");
//                covInfraStmt.setString(1, (String)temp.get("id"));
//                for(int x=0; x<entitiesCovered.size(); x++) {
//                    covInfraStmt.setString(2,((JSONObject)entitiesCovered.get(x)).get("name").toString());
//                    covInfraStmt.executeUpdate();
//                }
//
//            }

            // Adding Observations
            String wifiInsert = "INSERT INTO WiFiAPObservation " +
                    " VALUES (?, ?, ?, ?)";

            String wemoInsert = "INSERT INTO WeMoObservation " +
                    " VALUES (?, ?, ?, ?, ?)";

            String temperatureInsert = "INSERT INTO ThermometerObservation " +
                    " VALUES (?, ?, ?, ?)";

            BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
                    Observation.class);
            Observation obs = null;

            PreparedStatement wifiStmt = connection.prepareStatement(wifiInsert);
            PreparedStatement wemoStmt = connection.prepareStatement(wemoInsert);
            PreparedStatement temStmt = connection.prepareStatement(temperatureInsert);

            int batchSize = 10000;
            int wifiCount = 0, wemoCount = 0, thermoCount = 0, count = 0;
            while ((obs = reader.readNext()) != null) {

                if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                    temStmt.setInt(4, obs.getPayload().get("temperature").getAsInt());
                    temStmt.setString(3, obs.getSensor().getId());
                    temStmt.setTimestamp(2, new Timestamp(obs.getTimeStamp().getTime()));
                    temStmt.setString(1, obs.getId());
                    temStmt.addBatch();
                    thermoCount ++;
                } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                    wifiStmt.setString(4, obs.getPayload().get("clientId").getAsString());
                    wifiStmt.setString(3, obs.getSensor().getId());
                    wifiStmt.setTimestamp(2, new Timestamp(obs.getTimeStamp().getTime()));
                    wifiStmt.setString(1, obs.getId());
                    wifiStmt.addBatch();
                    wifiCount ++;
                } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                    wemoStmt.setInt(4, obs.getPayload().get("currentMilliWatts").getAsInt());
                    wemoStmt.setInt(5, obs.getPayload().get("onTodaySeconds").getAsInt());
                    wemoStmt.setString(3, obs.getSensor().getId());
                    wemoStmt.setTimestamp(2, new Timestamp(obs.getTimeStamp().getTime()));
                    wemoStmt.setString(1, obs.getId());
                    wemoStmt.addBatch();
                    wemoCount ++;
                }

                if (wemoCount % batchSize == 0)
                    wemoStmt.executeBatch();
                if (wifiCount % batchSize == 0)
                    wifiStmt.executeBatch();
                if (thermoCount % batchSize == 0)
                    temStmt.executeBatch();

                if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
                count ++;
            }
            wemoStmt.executeBatch();
            wifiStmt.executeBatch();
            temStmt.executeBatch();



        }
        catch(Exception e) {}
//        catch(ParseException | SQLException | IOException e) {
//            e.printStackTrace();
//        }

    }

    public void     addDevices() {

        PreparedStatement stmt;
        String insert;

        try {
            // Adding PlatformTypes
            insert = "INSERT INTO PLATFORM_TYPE" + " VALUES (?, ?, ?)";
            JSONArray platformType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT_TYPE.getPath())));

            for(int i =0;i<platformType_list.size();i++){
                stmt = connection.prepareStatement(insert);

                JSONObject temp=(JSONObject)platformType_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(3, (String)temp.get("name"));
                stmt.setString(2, (String)temp.get("description"));
                stmt.executeUpdate();

            }

            // Adding Platforms
            insert = "INSERT INTO PLATFORM " + " VALUES (?, ?, ?, ?, ?)";
            JSONArray platform_list = (JSONArray)parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT.getPath())));
            stmt = connection.prepareStatement(insert);

            int count = 0, batchSize = 1000;
            for(int i =0;i<platform_list.size();i++){
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

    public void addVSAndSemanticObservations() {

        PreparedStatement stmt;
        String insert;
        try {

            // Adding Semantic Observation Types
            insert = "INSERT INTO SEMANTIC_OBSERVATION_TYPE" + " VALUES (?, ?, ?)";
            JSONArray soType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SO_TYPE.getPath())));

            stmt = connection.prepareStatement(insert);
            for(int i =0;i<soType_list.size();i++){
                JSONObject temp=(JSONObject)soType_list.get(i);

                stmt.setString(1, temp.get("id").toString());
                stmt.setString(2, temp.get("name").toString());
                stmt.setString(3, temp.get("description").toString());
                stmt.executeUpdate();
            }

            // Adding Virtual Sensor Types
            insert = "INSERT INTO VIRTUAL_SENSOR_TYPE" +" VALUES (?, ?, ?, ?, ?)";
            JSONArray vsensorType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.VS_TYPE.getPath())));

            stmt = connection.prepareStatement(insert);
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
            insert = "INSERT INTO VIRTUAL_SENSOR " +
                    " VALUES (?, ?, ?, ?, ?, ?)";
            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.VS.getPath())));

            stmt = connection.prepareStatement(insert);
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

            // Adding Semantic Observations
            BigJsonReader<SemanticObservation> reader = new BigJsonReader<>(dataDir + DataFiles.SO.getPath(),
                    SemanticObservation.class);
            SemanticObservation sobs = null;

            String presenceInsert = "INSERT INTO PRESENCE " +
                    " VALUES (?, ?, ?, ?, ?)";

            String occupancyInsert = "INSERT INTO OCCUPANCY " +
                    " VALUES (?, ?, ?, ?, ?)";


            PreparedStatement presenceStmt = connection.prepareStatement(presenceInsert);
            PreparedStatement occupancyStmt = connection.prepareStatement(occupancyInsert);

            int batchSize = 10000;
            int presenceCount = 0, occupancyCount = 0, count = 0;
            while ((sobs = reader.readNext()) != null) {

                if (sobs.getType_().getId().equals("presence")) {
                    presenceStmt.setString(3, sobs.getPayload().get("location").getAsString());
                    presenceStmt.setString(5, sobs.getVirtualSensor().getId());
                    presenceStmt.setTimestamp(4, new Timestamp(sobs.getTimeStamp().getTime()));
                    presenceStmt.setString(1, sobs.getId());
                    presenceStmt.setString(2, sobs.getSemanticEntity().get("id").getAsString());
                    presenceStmt.addBatch();
                    presenceCount ++;
                } else if (sobs.getType_().getId().equals("occupancy")) {
                    stmt = occupancyStmt;
                    occupancyStmt.setInt(3, sobs.getPayload().get("occupancy").getAsInt());
                    occupancyStmt.setString(5, sobs.getVirtualSensor().getId());
                    occupancyStmt.setTimestamp(4, new Timestamp(sobs.getTimeStamp().getTime()));
                    occupancyStmt.setString(1, sobs.getId());
                    occupancyStmt.setString(2, sobs.getSemanticEntity().get("id").getAsString());
                    occupancyStmt.addBatch();
                    occupancyCount ++;
                }

                if (presenceCount % batchSize == 0)
                    presenceStmt.executeBatch();
                if (occupancyCount % batchSize == 0)
                    occupancyStmt.executeBatch();

                if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s S Observations", count));
                count ++;
            }
            presenceStmt.executeBatch();
            occupancyStmt.executeBatch();


        }
        catch(ParseException | IOException | SQLException e) {
            e.printStackTrace();
        }

    }

    public void insertPerformance() {
        PreparedStatement stmt;
        String insert;

        try {
            String wifiInsert = "INSERT INTO WiFiAPObservation " +
                    " VALUES (?, ?, ?, ?)";

            String wemoInsert = "INSERT INTO WeMoObservation " +
                    " VALUES (?, ?, ?, ?, ?)";

            String temperatureInsert = "INSERT INTO ThermometerObservation " +
                    " VALUES (?, ?, ?, ?)";

            BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.INSERT_TEST.getPath(),
                    Observation.class);
            Observation obs = null;

            PreparedStatement wifiStmt = connection.prepareStatement(wifiInsert);
            PreparedStatement wemoStmt = connection.prepareStatement(wemoInsert);
            PreparedStatement temStmt = connection.prepareStatement(temperatureInsert);

            int batchSize = 10000;
            int wifiCount = 0, wemoCount = 0, thermoCount = 0;
            while ((obs = reader.readNext()) != null) {

                if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                    temStmt.setInt(4, obs.getPayload().get("temperature").getAsInt());
                    temStmt.setString(3, obs.getSensor().getId());
                    temStmt.setTimestamp(2, new Timestamp(obs.getTimeStamp().getTime()));
                    temStmt.setString(1, obs.getId());
                    temStmt.addBatch();
                    thermoCount ++;
                } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                    wifiStmt.setString(4, obs.getPayload().get("clientId").getAsString());
                    wifiStmt.setString(3, obs.getSensor().getId());
                    wifiStmt.setTimestamp(2, new Timestamp(obs.getTimeStamp().getTime()));
                    wifiStmt.setString(1, obs.getId());
                    wifiStmt.addBatch();
                    wifiCount ++;
                } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                    wemoStmt.setInt(4, obs.getPayload().get("currentMilliWatts").getAsInt());
                    wemoStmt.setInt(5, obs.getPayload().get("onTodaySeconds").getAsInt());
                    wemoStmt.setString(3, obs.getSensor().getId());
                    wemoStmt.setTimestamp(2, new Timestamp(obs.getTimeStamp().getTime()));
                    wemoStmt.setString(1, obs.getId());
                    wemoStmt.addBatch();
                    wemoCount ++;
                }

                if (wemoCount % batchSize == 0)
                    wemoStmt.executeBatch();
                if (wifiCount % batchSize == 0)
                    wifiStmt.executeBatch();
                if (thermoCount % batchSize == 0)
                    temStmt.executeBatch();
            }
            wemoStmt.executeBatch();
            wifiStmt.executeBatch();
            temStmt.executeBatch();

        }
        catch(SQLException e) {
            e.printStackTrace();
        }

    }

}

