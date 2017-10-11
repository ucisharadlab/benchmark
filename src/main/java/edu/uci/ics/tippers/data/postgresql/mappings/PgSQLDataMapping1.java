package edu.uci.ics.tippers.data.postgresql.mappings;

import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.data.postgresql.PgSQLBaseDataMapping;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.model.observation.Observation;
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

public class PgSQLDataMapping1 extends PgSQLBaseDataMapping {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private JSONParser parser = new JSONParser();

    public PgSQLDataMapping1(Connection connection, String dataDir) {
        super(connection, dataDir);
    }

    public void addAll() throws BenchmarkException{
        try {
            connection.setAutoCommit(false);
            addMetadata();
            addDevices();
            addSensorsAndObservations();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void addMetadata() throws BenchmarkException {

        PreparedStatement stmt;
        String insert;

        try {

            // Adding Groups
            insert ="INSERT INTO USER_GROUP " + "(ID, NAME, DESCRIPTION) VALUES (?, ?, ?)";
            JSONArray group_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.GROUP.getPath())));
            stmt = connection.prepareStatement(insert);

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
            stmt = connection.prepareStatement(insert);

            String membership = "INSERT INTO USER_GROUP_MEMBERSHIP " + "(USER_ID, USER_GROUP_ID) VALUES (?, ?)";
            PreparedStatement memStmt = connection.prepareStatement(membership);

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
                    "(ID, NAME, DESCRIPTION) VALUES (?, ?, ?)";
            JSONArray infraType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA_TYPE.getPath())));

            stmt = connection.prepareStatement(insert);
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

            stmt = connection.prepareStatement(insert);
            for(int i =0;i<infra_list.size();i++){
                JSONObject temp=(JSONObject)infra_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(3, (String)temp.get("name"));
                stmt.setString(2, (String)((JSONObject)temp.get("type_")).get("id"));
                stmt.setInt(4, ((Number)temp.get("floor")).intValue());
                stmt.executeUpdate();
            }

            String regionInfra = "INSERT INTO INFRASTRUCTURE_LOCATION " + "(LOCATION_ID, INFRASTRUCTURE_ID) VALUES (?, ?)";
            PreparedStatement regionInfraStmt = connection.prepareStatement(membership);


            for(int i =0;i<infra_list.size();i++){
                JSONObject temp=(JSONObject)infra_list.get(i);
                JSONArray locations;

                locations = (JSONArray)temp.get("geometry");
                regionInfraStmt.setString(2, (String)temp.get("id"));
                for(int x=0; x<locations.size(); x++) {
                    regionInfraStmt.setString(1, ((JSONObject) locations.get(x)).get("id").toString());
                }
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
            insert = "INSERT INTO SENSOR_TYPE " +
                    "(ID, NAME, DESCRIPTION, MOBILITY, CAPTURE_FUNCTIONALITY, PAYLOAD_SCHEMA) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";
            JSONArray sensorType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR_TYPE.getPath())));
            stmt = connection.prepareStatement(insert);
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

            PreparedStatement covInfraStmt = connection.prepareStatement(covInfrastructure);

            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR.getPath())));

            stmt = connection.prepareStatement(insert);
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

            // Adding Observations
            insert = "INSERT INTO OBSERVATION " +
                    "(ID, PAYLOAD, TIMESTAMP, SENSOR_ID) VALUES (?, ?, ?, ?)";

            BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
                    Observation.class);
            Observation obs = null;

            stmt = connection.prepareStatement(insert);
            while ((obs = reader.readNext()) != null) {

                stmt.setString(4, obs.getSensor().getId());
                stmt.setTimestamp(3, new Timestamp(obs.getTimeStamp().getTime()));
                stmt.setString(1, obs.getId());
                stmt.setString(2, obs.getPayload().toString());

                stmt.executeUpdate();
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
                stmt = connection.prepareStatement(insert);

                JSONObject temp=(JSONObject)platformType_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(3, (String)temp.get("description"));
                stmt.executeUpdate();

            }

            // Adding Platforms
            insert = "INSERT INTO PLATFORM " + "(ID, NAME, " +
                    "USER_ID, PLATFORM_TYPE_ID) VALUES (?, ?, ?, ?)";
            JSONArray platform_list = (JSONArray)parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.PLT.getPath())));

            for(int i =0;i<platform_list.size();i++){
                stmt = connection.prepareStatement(insert);
                JSONObject temp=(JSONObject)platform_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(3, (String)temp.get("ownerId"));
                stmt.setString(4, (String)temp.get("typeId"));
                stmt.executeUpdate();

            }

        }
        catch(ParseException | SQLException | IOException e) {
            e.printStackTrace();
        }
    }

//    public void addPresenceAndOccupancyData() thstmts GSException {
//
//        Collection<String, Row> collection;
//        Row stmt;
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
//                JSONObject (String)temp=(JSONObject)presence_list.get(i);
//                collection = gridStore.getCollection("Presence");
//
//                stmt = collection.createRow();
//                stmt.setString(0, (String)temp.get("id"));
//                stmt.setString(1, (String)temp.get("locationid"));
//                stmt.setString(2, (String)temp.get("userid"));
//                stmt.setString(3, (String)temp.get("timestamp"));
//                collection.put(stmt);
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
//                JSONObject (String)temp=(JSONObject)occupancy_list.get(i);
//                collection = gridStore.getCollection("Occupancy");
//
//                stmt = collection.createRow();
//                stmt.setString(0, (String)temp.get("id"));
//                stmt.setString(1, (String)temp.get("infraid"));
//                stmt.setString(2, (String)temp.get("occupancy"));
//                stmt.setString(3, (String)temp.get("timestamp"));
//                collection.put(stmt);
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

//    public void addVSAndSemanticObservations() thstmts GSException {
//
//        Collection<String, Row> collection;
//        Row stmt;
//
//        // Adding SemanticObservationTypes
//        collection = gridStore.getCollection("User");
//        stmt = collection.createRow();
//        stmt.setString(0, "user1");
//        stmt.setString(1, "peeyushg@uci.edu");
//        stmt.setString(2, "Peeyush");
//        stmt.setString(3, new String[]{"group1"});
//
//        collection.put(stmt);
//
//        // Adding Virtual Sensor Types
//        collection = gridStore.getCollection("User");
//        stmt = collection.createRow();
//        stmt.setString(0, "user1");
//        stmt.setString(1, "peeyushg@uci.edu");
//        stmt.setString(2, "Peeyush");
//        stmt.setString(3, new String[]{"group1"});
//
//        collection.put(stmt);
//
//        // Adding Virtual Sensors
//        collection = gridStore.getCollection("User");
//        stmt = collection.createRow();
//        stmt.setString(0, "user1");
//        stmt.setString(1, "peeyushg@uci.edu");
//        stmt.setString(2, "Peeyush");
//        stmt.setString(3, new String[]{"group1"});
//
//        collection.put(stmt);
//
//        //  TODO: Adding dynamic semantic observations
//
//    }

}

