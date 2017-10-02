package edu.uci.ics.tippers.data.postgresql.mappings;

import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.data.postgresql.PgSQLBaseDataMapping;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.model.observation.ObservationRow;
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
        addMetadata();
        addDevices();
        // addPresenceAndOccupancyData();
        addSensorsAndObservations();
    }

    public void addSemanticEntity(PreparedStatement stmt, String id) throws BenchmarkException{
        try {
            stmt.setString(1, id);
            stmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Adding Data");
        }
    }

    public void addMetadata() throws BenchmarkException {

        PreparedStatement stmt;
        String insert;

        try {

            String seInsert = "INSERT INTO SEMANTIC_ENTITY " +
                    "(ID) VALUES (?)";
            PreparedStatement seStmt = connection.prepareStatement(seInsert);

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

                addSemanticEntity(seStmt, (String)temp.get("id"));

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("emailId"));
                stmt.setString(4, (String)temp.get("name"));
                stmt.setString(3, (String)temp.get("googleAuthToken"));

                stmt.executeUpdate();


                groups=(JSONArray)temp.get("groupIds");
                memStmt.setString(1, (String)temp.get("id"));
                for(int x=0;x<groups.size();x++) {
                    memStmt.setString(2, groups.get(x).toString());
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

            // Adding Regions
            insert = "INSERT INTO REGION " +
                    "(ID, FLOOR, NAME) VALUES (?, ?, ?)";
            JSONArray region_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.REGION.getPath())));
            stmt = connection.prepareStatement(insert);

            String regionInfra = "INSERT INTO REGION_LOCATION " + "(LOCATION_ID, REGION_ID) VALUES (?, ?)";
            PreparedStatement regionInfraStmt = connection.prepareStatement(membership);


            for(int i =0;i<region_list.size();i++){
                JSONObject temp=(JSONObject)region_list.get(i);
                JSONArray locations;

                stmt.setString(1, (String)temp.get("id"));
                stmt.setDouble(2, ((Number)temp.get("floor")).doubleValue());
                stmt.setString(3, (String)temp.get("name"));
                stmt.executeUpdate();

                locations = (JSONArray)temp.get("geometry");
                regionInfraStmt.setString(2, (String)temp.get("id"));
                for(int x=0; x<locations.size(); x++) {
                    regionInfraStmt.setString(1, ((JSONObject) locations.get(x)).get("id").toString());
                }
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
                    "(ID, INFRASTRUCTURE_TYPE_ID, NAME, REGION_ID) VALUES (?, ?, ?, ?)";

            JSONArray infra_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.INFRA.getPath())));

            stmt = connection.prepareStatement(insert);
            for(int i =0;i<infra_list.size();i++){
                JSONObject temp=(JSONObject)infra_list.get(i);

                addSemanticEntity(seStmt, (String)temp.get("id"));

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(3, (String)temp.get("name"));
                stmt.setString(2, (String)((JSONObject)temp.get("type_")).get("id"));
                stmt.setString(4, (String)((JSONObject)temp.get("region")).get("id"));
                stmt.executeUpdate();

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

            // Adding ObservationTypes
            insert = "INSERT INTO OBSERVATION_TYPE " +
                    "(ID, NAME, DESCRIPTION, PAYLOAD_SCHEMA) VALUES (?, ?, ?, ?)";
            JSONArray obsType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.OBS_TYPE.getPath())));

            stmt = connection.prepareStatement(insert);
            for(int i =0;i<obsType_list.size();i++){

                JSONObject temp=(JSONObject)obsType_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(3, (String)temp.get("description"));
                stmt.setString(4, (String)temp.get("payloadSchema"));
                stmt.executeUpdate();

            }

            // Adding Sensor Types
            insert = "INSERT INTO SENSOR_TYPE " +
                    "(ID, NAME, DESCRIPTION, MOBILITY, OBSERVATION_TYPE_ID) " +
                    "VALUES (?, ?, ?, ?, ?)";
            JSONArray sensorType_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR_TYPE.getPath())));
            stmt = connection.prepareStatement(insert);
            for(int i =0;i<sensorType_list.size();i++){
                JSONObject temp=(JSONObject)sensorType_list.get(i);

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(3, (String)temp.get("description"));
                stmt.setString(4, (String)temp.get("mobility"));
                stmt.setString(5, (String)temp.get("observationTypeId"));

                stmt.executeUpdate();

            }



            // Adding Sensors
            insert = "INSERT INTO SENSOR" +
                    "(ID, NAME, COVERAGE_ID, INFRASTRUCTURE_ID, USER_ID, SENSOR_TYPE_ID) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";

            String senCoverage = "INSERT INTO SENSOR_COVERAGE" +
                    "(ID, RADIUS) " +
                    "VALUES (?, ?)";
            String covInfrastructure = "INSERT INTO Coverage_Infrastructure" +
                    "(ID, INFRASTRUCTURE_ID) " +
                    "VALUES (?, ?)" ;

            PreparedStatement senCoverageStmt = connection.prepareStatement(senCoverage);
            PreparedStatement covInfraStmt = connection.prepareStatement(covInfrastructure);

            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR.getPath())));

            stmt = connection.prepareStatement(insert);
            for(int i =0;i<sensor_list.size();i++){
                JSONObject temp=(JSONObject)sensor_list.get(i);
                JSONArray locations;

                senCoverageStmt.setString(1, (String)((JSONObject)temp.get("coverage")).get("id"));
                senCoverageStmt.setDouble(2, 0);
                senCoverageStmt.executeUpdate();

                JSONArray entitiesCovered = (JSONArray)((JSONObject)temp.get("coverage")).get("entitiesCovered");
                covInfraStmt.setString(1, (String)((JSONObject)temp.get("coverage")).get("id"));
                for(int x=0; x<entitiesCovered.size(); x++) {
                    covInfraStmt.setString(2,((JSONObject)entitiesCovered.get(x)).get("name").toString());
                    covInfraStmt.executeUpdate();
                }

                stmt.setString(1, (String)temp.get("id"));
                stmt.setString(2, (String)temp.get("name"));
                stmt.setString(6, (String)((JSONObject)temp.get("sensorType")).get("id"));
                stmt.setString(4, (String)((JSONObject)temp.get("infrastructure")).get("id"));
                stmt.setString(5, (String)((JSONObject)temp.get("owner")).get("id"));
                // stmt.setString(5, (String)temp.get("platform"));
                stmt.setString(3, (String)((JSONObject)temp.get("coverage")).get("id"));
                //stmt.setString(7, (String)temp.get("sensorConfig"));
                stmt.executeUpdate();


            }

            // Adding Observations
            insert = "INSERT INTO OBSERVATION " +
                    "(ID, PAYLOAD, TIMESTAMP, SENSOR_ID, OBSERVATION_TYPE_ID) VALUES (?, ?, ?, ?, ?)";

            BigJsonReader<ObservationRow> reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
                    ObservationRow.class);
            ObservationRow obs = null;

            stmt = connection.prepareStatement(insert);
            while ((obs = reader.readNext()) != null) {

                stmt.setString(4, obs.getSensorId());
                stmt.setTimestamp(3, new Timestamp(obs.getTimeStamp().getTime()));
                stmt.setString(1, obs.getId());
                stmt.setString(5, obs.getTypeId());
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

