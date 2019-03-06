package edu.uci.ics.tippers.experiments;

import com.toshiba.mwcloud.gs.*;
import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.TimeUnit;
import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.connection.griddb.StoreManager;
import edu.uci.ics.tippers.connection.postgresql.PgSQLConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.execution.Configuration;
import edu.uci.ics.tippers.model.observation.Observation;
import edu.uci.ics.tippers.query.QueryCSVReader;
import edu.uci.ics.tippers.writer.RowWriter;
import org.apache.log4j.Logger;
import org.ini4j.Ini;
import org.ini4j.IniPreferences;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

import static edu.uci.ics.tippers.common.util.Helper.getFileFromQueryWithVersion;
import static edu.uci.ics.tippers.common.util.Helper.getFileFromQuery;


public class JoinPerformance {

    private static final Logger LOGGER = Logger.getLogger(JoinPerformance.class);

    private Connection pgConnection;
    private GridStore gridStore;
    private Configuration configuration;

    protected boolean writeOutput;
    protected String queriesDir;
    protected String dataDir;
    protected String outputDir;
    protected long timeout;

    private EnumSet<IndexType> indexSet = EnumSet.of(IndexType.HASH);
    private EnumSet<IndexType> treeIndex = EnumSet.of(IndexType.TREE);

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private JSONParser parser = new JSONParser();


    private JoinPerformance() {
        pgConnection = PgSQLConnectionManager.getInstance().getConnection();
        gridStore = StoreManager.getInstance().getGridStore();

        configuration = readConfiguration();

        this.writeOutput = configuration.isWriteOutput();
        this.outputDir = configuration.getOutputDir();
        this.queriesDir = configuration.getQueriesDir();
        this.dataDir = configuration.getDataDir();
        this.timeout = configuration.getTimeout();
    }

    private void createPgSchema() {

        String createSensorTable = "CREATE TABLE SENSOR_EXP (\n" +
                "  ID varchar(255) NOT NULL,\n" +
                "  NAME varchar(255) DEFAULT NULL,\n" +
                "  INFRASTRUCTURE_ID varchar(255) DEFAULT NULL,\n" +
                "  USER_ID varchar(255) DEFAULT NULL,\n" +
                "  SENSOR_TYPE_ID varchar(255) DEFAULT NULL,\n" +
                "  SENSOR_CONFIG varchar(255) DEFAULT NULL,\n" +
                "  PRIMARY KEY (ID)\n" +
                ") ";

        String createCoverageTable = "CREATE TABLE COVERAGE_INFRASTRUCTURE_EXP (\n" +
                "  SENSOR_ID varchar(255) NOT NULL,\n" +
                "  INFRASTRUCTURE_ID varchar(255) NOT NULL\n" +
                ") ";

        String createObservationTable = "CREATE TABLE OBSERVATION_EXP (\n" +
                "  id varchar(255),\n" +
                "  payload json ,\n" +
                "  timeStamp timestamp,\n" +
                "  sensor_id varchar(255),\n" +
                "  PRIMARY KEY (ID)\n" +
                ") ";

        try {
            PreparedStatement stmt = pgConnection.prepareStatement(createSensorTable);
            stmt.executeUpdate();

            stmt = pgConnection.prepareStatement(createCoverageTable);
            stmt.executeUpdate();

            stmt = pgConnection.prepareStatement(createObservationTable);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void dropPgSchema() {

        String dropSensorTable = " DROP TABLE IF EXISTS SENSOR_EXP;";
        String dropCoverageTable = " DROP TABLE IF EXISTS COVERAGE_INFRASTRUCTURE_EXP;";
        String dropObservationTable = " DROP TABLE IF EXISTS OBSERVATION_EXP;";

        try {
            PreparedStatement stmt = pgConnection.prepareStatement(dropCoverageTable);
            stmt.executeUpdate();

            stmt = pgConnection.prepareStatement(dropObservationTable);
            stmt.executeUpdate();

            stmt = pgConnection.prepareStatement(dropSensorTable);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    private void addPgData() {

        PreparedStatement stmt;
        String insert;

        try {

            // Adding Sensors
            insert = "INSERT INTO SENSOR_EXP" +
                    "(ID, NAME, INFRASTRUCTURE_ID, USER_ID, SENSOR_TYPE_ID, SENSOR_CONFIG) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";


            String covInfrastructure = "INSERT INTO Coverage_Infrastructure_Exp" +
                    "(SENSOR_ID, INFRASTRUCTURE_ID) " +
                    "VALUES (?, ?)" ;

            PreparedStatement covInfraStmt = pgConnection.prepareStatement(covInfrastructure);

            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + "sensor.json")));

            stmt = pgConnection.prepareStatement(insert);
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
            insert = "INSERT INTO OBSERVATION_EXP " +
                    "(ID, PAYLOAD, TIMESTAMP, SENSOR_ID) VALUES (?, ?::JSON, ?, ?)";

            BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + "observation.json",
                    Observation.class);
            Observation obs = null;

            stmt = pgConnection.prepareStatement(insert);
            int count = 0;
            while ((obs = reader.readNext()) != null) {

                stmt.setString(4, obs.getSensor().getId());
                stmt.setTimestamp(3, new Timestamp(obs.getTimeStamp().getTime()));
                stmt.setString(1, obs.getId());
                stmt.setObject(2, obs.getPayload().toString());
                stmt.addBatch();

                count ++;
                if (count % Constants.PGSQL_BATCH_SIZE == 0)
                    stmt.executeBatch();

                if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
            }
            stmt.executeBatch();

        }
        catch(ParseException | SQLException | IOException e) {
            e.printStackTrace();
        }

    }

    private void createGridDBSchema()  {
        ContainerInfo containerInfo = new ContainerInfo();
        List<ColumnInfo> columnInfoList = new ArrayList<>();

        // Creating Sensors
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("Sensor_Exp");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("typeId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("infrastructureId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("ownerId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("coverage", GSType.STRING_ARRAY));
        columnInfoList.add(new ColumnInfo("sensorConfig", GSType.STRING));


        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        try {
            gridStore.putCollection("Sensor_Exp", containerInfo, true);
        } catch (GSException e) {
            e.printStackTrace();
        }

        // Creating Observation Containers, One per each type
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("WiFiAPObservation_Exp");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("timeStamp", GSType.TIMESTAMP, treeIndex));
        columnInfoList.add(new ColumnInfo("sensorId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("clientId", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        try {
            gridStore.putCollection("WiFiAPObservation_Exp", containerInfo, true);
        } catch (GSException e) {
            e.printStackTrace();
        }

        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("WeMoObservation_Exp");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("timeStamp", GSType.TIMESTAMP, treeIndex));
        columnInfoList.add(new ColumnInfo("sensorId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("currentMilliWatts", GSType.INTEGER));
        columnInfoList.add(new ColumnInfo("onTodaySeconds", GSType.INTEGER));


        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        try {
            gridStore.putCollection("WeMoObservation_Exp", containerInfo, true);
        } catch (GSException e) {
            e.printStackTrace();
        }

        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("ThermometerObservation_Exp");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("timeStamp", GSType.TIMESTAMP, treeIndex));
        columnInfoList.add(new ColumnInfo("sensorId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("temperature", GSType.INTEGER));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        try {
            gridStore.putCollection("ThermometerObservation_Exp", containerInfo, true);
        } catch (GSException e) {
            e.printStackTrace();
        }
    }

    private void dropGridDBSchema() {

    }

    public void addGridDBData() {
        Collection<String, Row> collection;
        Row row;
        try {

            // Adding Sensors
            JSONArray sensor_list = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + "sensor.json")));

            for(int i =0;i<sensor_list.size();i++){
                JSONObject temp=(JSONObject)sensor_list.get(i);
                JSONArray locations;
                collection = gridStore.getCollection("Sensor_Exp");

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
            BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + "observation.json",
                    Observation.class);
            Observation obs = null;
            int count = 0;

            while ((obs = reader.readNext()) != null) {
                String collectionName = null;
                if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                    collectionName = "WiFiAPObservation_Exp";
                } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                    collectionName = "WeMoObservation_Exp";
                } else if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                    collectionName = "ThermometerObservation_Exp";
                }

                collection = gridStore.getCollection(collectionName);

                row = collection.createRow();
                row.setValue(1, obs.getTimeStamp());
                row.setValue(0, obs.getId());
                row.setValue(2, obs.getSensor().getId());

                if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                    row.setValue(3, obs.getPayload().get("temperature").getAsInt());
                } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                    row.setValue(3, obs.getPayload().get("clientId").getAsString());
                } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                    row.setValue(3, obs.getPayload().get("currentMilliWatts").getAsInt());
                    row.setValue(4, obs.getPayload().get("onTodaySeconds").getAsInt());
                }
                collection.put(row);
                if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
                count ++;
            }

        }
        catch(ParseException | IOException e) {
            e.printStackTrace();
        }

    }

    private Duration runWithThread(Callable<Duration> query) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Duration> future = executorService.submit(query);
        try {
            return future.get(timeout, java.util.concurrent.TimeUnit.MILLISECONDS);
        }catch (TimeoutException e) {
            e.printStackTrace();
            throw new BenchmarkException("Query Timed Out");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        } finally {
            executorService.shutdown();
            try {
                executorService.awaitTermination(Constants.SHUTDOWN_WAIT, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cleanUp();
        }
    }

    private void cleanUp(){

    }

    private Duration runPostgreSQLTimedQuery(PreparedStatement stmt, int queryNum) throws BenchmarkException {
        try {
            Instant start = Instant.now();
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            RowWriter<String> writer = new RowWriter<>(outputDir, Database.POSTGRESQL, 3,
                    getFileFromQuery(queryNum));
            while(rs.next()) {
                if (writeOutput) {
                    StringBuilder line = new StringBuilder("");
                    for(int i = 1; i <= columnsNumber; i++)
                        line.append(rs.getString(i)).append("\t");
                    writer.writeString(line.toString());
                }
            }
            writer.close();
            rs.close();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }

    private List<List<Object>> runPGQueryWithRows(PreparedStatement stmt) throws BenchmarkException {
        try {
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            List<List<Object>> rows = new ArrayList<>();

            while(rs.next()) {
                List<Object> row = new ArrayList<>();
                for(int i = 1; i <= columnsNumber; i++)
                    row.add(rs.getObject(i));
                rows.add(row);
            }
            rs.close();
            return rows;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
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

    private Duration runPGBigJoinQuery(String userId, Date startTime, Date endTime){

        String query = "SELECT u.name, s1.location " +
                "FROM PRESENCE s1, PRESENCE s2, USERS u " +
                "WHERE s1.timeStamp > ?  and s1.timestamp < ?" +
                "AND s2.timeStamp = s1.timeStamp " +
                "AND s1.semantic_entity_id = ? AND s1.semantic_entity_id != s2.semantic_entity_id " +
                "AND s2.semantic_entity_id = u.id AND s1.location = s2.location ";
        try {
            PreparedStatement stmt = pgConnection.prepareStatement(query);
            stmt.setDate (1, new java.sql.Date(startTime.getTime()));
            stmt.setDate (2, new java.sql.Date(startTime.getTime()));
            stmt.setString(3, userId);

            return runPostgreSQLTimedQuery(stmt, 12);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }

    private Duration runPGBigAppJoinQuery(String userId, Date startTime, Date endTime){

        Instant start = Instant.now();

        try {
            String query = "SELECT id, name FROM User";
            PreparedStatement stmt = pgConnection.prepareStatement(query);

            Map<String, String> userMap = runPGQueryWithRows(stmt)
                    .stream().collect(Collectors.toMap(e -> {
                        return (String)e.get(0);
                    }, e -> {
                        return (String)e.get(1);
                    }));

            RowWriter<String> writer = new RowWriter<>(outputDir, Database.POSTGRESQL, 3, getFileFromQuery(12));

            Container<String, Row> container = gridStore.getContainer("Presence");
            query = String.format("SELECT * FROM Presence WHERE semanticEntityId = '%s' " +
                            "AND timeStamp >= TIMESTAMP('%s') AND timeStamp <= TIMESTAMP('%s')",
                    userId, sdf.format(startTime), sdf.format(endTime));
            stmt = pgConnection.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            List<List<Object>> rows = new ArrayList<>();

            while(rs.next()) {

                List<Object> row = new ArrayList<>();
                for(int i = 1; i <= columnsNumber; i++)
                    row.add(rs.getObject(i));

                query = String.format("SELECT * FROM Presence WHERE timeStamp = TIMESTAMP('%s') " +
                                "AND location='%s' AND semanticEntityId != '%s'", sdf.format(row.get(1)),
                        row.get(3), userId);
                stmt = pgConnection.prepareStatement(query);
                List<List<Object>> observations = runPGQueryWithRows(stmt);

                observations.forEach(e->{
                    if (writeOutput) {
                        try {
                            writer.writeString(userMap.get(e.get(4)) + ", " +e.get(3));
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }

                    }
                });
            }
            rs.close();
            writer.close();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }

    private Duration runGridDBBigJoinQuery(String userId, Date startTime, Date endTime){
        Instant start = Instant.now();

        try {
            Map<String, String> userMap = runQueryWithRows("User",
                    "SELECT * FROM User")
                    .stream().collect(Collectors.toMap(e -> {
                        try {
                            return e.getString(0);
                        } catch (GSException e1) {
                            e1.printStackTrace();
                            return null;
                        }
                    }, e -> {
                        try {
                            return e.getString(2);
                        } catch (GSException e1) {
                            e1.printStackTrace();
                            return null;
                        }
                    }));

            RowWriter<String> writer = new RowWriter<>(outputDir, Database.GRIDDB, 3, getFileFromQuery(12));

            Container<String, Row> container = gridStore.getContainer("Presence");
            Query<Row> gridDBQuery = container.query(String.format("SELECT * FROM Presence WHERE semanticEntityId = '%s' " +
                            "AND timeStamp >= TIMESTAMP('%s') AND timeStamp <= TIMESTAMP('%s')",
                    userId, sdf.format(startTime), sdf.format(endTime)));
            RowSet<Row> rows = gridDBQuery.fetch();

            while (rows.hasNext()) {

                Row row = rows.next();

                String query = String.format("SELECT * FROM Presence WHERE timeStamp = TIMESTAMP('%s') " +
                                "AND location='%s' AND semanticEntityId != '%s'", sdf.format(row.getTimestamp(1)),
                        row.getString(3), userId);
                List<Row> observations = runQueryWithRows("Presence", query);

                observations.forEach(e->{
                    if (writeOutput) {
                        try {
                            writer.writeString(userMap.get(e.getString(4)) + ", " +e.getString(3));
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }

                    }
                });
            }
            writer.close();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }
    }

    private Duration runPGQuery(List<String> sensorNames, Date startTime, Date endTime) throws BenchmarkException {

        String query = "SELECT obs.timeStamp, obs.payload FROM OBSERVATION_EXP obs INNER JOIN SENSOR_EXP sen ON sen.id=obs.sensor_Id " +
                "WHERE obs.timestamp>? AND obs.timestamp<? AND sen.name=ANY(?)";
        try {
            PreparedStatement stmt = pgConnection.prepareStatement(query);
            stmt.setTimestamp(1, new Timestamp(startTime.getTime()));
            stmt.setTimestamp(2, new Timestamp(endTime.getTime()));

            Array sensorIdArray = pgConnection.createArrayOf("VARCHAR", sensorNames.toArray());
            stmt.setArray(3, sensorIdArray);
            return runPostgreSQLTimedQuery(stmt, 11);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

    }

    private Duration runGridDBQuery(List<String> sensorNames, Date startTime, Date endTime) throws BenchmarkException {

        try {
            Instant start = Instant.now();
            List<String> wifiSensors = new ArrayList<>();
            List<String> wemoSensors = new ArrayList<>();
            List<String> thermoSensors = new ArrayList<>();

            RowWriter<String> writer = new RowWriter<>(outputDir, Database.GRIDDB, 3, getFileFromQuery(11));
            for (String sensorName : sensorNames) {
                List<Row> rows = runQueryWithRows("Sensor_Exp",
                        String.format("SELECT * FROM Sensor_Exp WHERE name='%s'", sensorName));
                if (rows.size() == 0) continue;

                String typeId = rows.get(0).getString(2);
                String id = rows.get(0).getString(0);
                if ("Thermometer".equals(typeId))
                    thermoSensors.add(id);
                else if ("WeMo".equals(typeId))
                    wemoSensors.add(id);
                else if ("WiFiAP".equals(typeId))
                    wifiSensors.add(id);
            }

            if (!thermoSensors.isEmpty()) {
                String query = String.format("SELECT * FROM ThermometerObservation_Exp WHERE timeStamp > TIMESTAMP('%s') " +
                        "AND timeStamp < TIMESTAMP('%s') AND ( "
                        + thermoSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                        + ");", sdf.format(startTime), sdf.format(endTime));
                List<Row> observations = runQueryWithRows("ThermometerObservation_Exp", query);

                observations.forEach(e -> {
                    if (writeOutput) {
                        ContainerInfo containerInfo = null;
                        try {
                            StringBuilder line = new StringBuilder("");
                            containerInfo = e.getSchema();
                            int columnCount = containerInfo.getColumnCount();
                            for (int i = 0; i < columnCount; i++) {
                                line.append(e.getValue(i)).append("\t");
                            }
                            writer.writeString(line.toString());
                        } catch (IOException e1) {
                            e1.printStackTrace();
                            throw new BenchmarkException("Error Running Query On GridDB");
                        }
                    }
                });
            }
            if (!wemoSensors.isEmpty()) {
                String query = String.format("SELECT * FROM WeMoObservation_Exp WHERE timeStamp > TIMESTAMP('%s') " +
                        "AND timeStamp < TIMESTAMP('%s') AND ( "
                        + wemoSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                        + ");", sdf.format(startTime), sdf.format(endTime));
                List<Row> observations = runQueryWithRows("WeMoObservation_Exp", query);

                observations.forEach(e -> {
                    if (writeOutput) {
                        ContainerInfo containerInfo = null;
                        try {
                            StringBuilder line = new StringBuilder("");
                            containerInfo = e.getSchema();
                            int columnCount = containerInfo.getColumnCount();
                            for (int i = 0; i < columnCount; i++) {
                                line.append(e.getValue(i)).append("\t");
                            }
                            writer.writeString(line.toString());
                        } catch (IOException e1) {
                            e1.printStackTrace();
                            throw new BenchmarkException("Error Running Query On GridDB");
                        }
                    }
                });
            }
            if (!wifiSensors.isEmpty()) {
                String query = String.format("SELECT * FROM WiFiAPObservation_Exp WHERE timeStamp > TIMESTAMP('%s') " +
                        "AND timeStamp < TIMESTAMP('%s') AND ( "
                        + wifiSensors.stream().map(e -> "sensorId = '" + e + "'" ).collect(Collectors.joining(" OR "))
                        + ");", sdf.format(startTime), sdf.format(endTime));
                List<Row> observations = runQueryWithRows("WiFiAPObservation_Exp", query);


                observations.forEach(e -> {
                    if (writeOutput) {
                        ContainerInfo containerInfo = null;
                        try {
                            StringBuilder line = new StringBuilder("");
                            containerInfo = e.getSchema();
                            int columnCount = containerInfo.getColumnCount();
                            for (int i = 0; i < columnCount; i++) {
                                line.append(e.getValue(i)).append("\t");
                            }
                            writer.writeString(line.toString());
                        } catch (IOException e1) {
                            e1.printStackTrace();
                            throw new BenchmarkException("Error Running Query On GridDB");
                        }
                    }
                });
            }
            writer.close();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        }

    }

    private List<Duration> runExperiment(int selectivity) {

//        System.out.println("Creating Schema And Adding Data");

//        dropGridDBSchema();gt
//        dropPgSchema();
//
//        createPgSchema();
//        createGridDBSchema();
//
//        addPgData();
//        addGridDBData();

//        System.out.println("Running Queries Now");
        System.out.println("Running Queries");

        System.out.println("Running PgSQL Small Join");
        int numQueries = 0;
        Duration runTime = Duration.ZERO;
        String values[];
        List<Duration> queryRunTimes = new ArrayList<>();
        QueryCSVReader reader = new QueryCSVReader(queriesDir + getFileFromQueryWithVersion(11, selectivity));
        try {

            while ((values = reader.readNextLine()) != null) {
                List<String> sensorNames = Arrays.asList(values[1].split(";"));
                Date start, end;
                start = sdf.parse(values[2]);
                end = sdf.parse(values[3]);
                runTime = runWithThread(()->runPGQuery(sensorNames, start, end));
                numQueries++;
                queryRunTimes.add(runTime);
                System.out.println(String.format("Postgres Count: %s Time: %s", numQueries, runTime.toString()));
            }
        } catch (Exception | Error e) {
            e.printStackTrace();
            queryRunTimes.add(Constants.MAX_DURATION);
        }

        System.out.println("Running GridDB Small App Join");
        numQueries = 0;
        reader = new QueryCSVReader(queriesDir + getFileFromQueryWithVersion(11, selectivity));
        try {
            while ((values = reader.readNextLine()) != null) {
                List<String> sensorNames = Arrays.asList(values[1].split(";"));
                Date start, end;
                start = sdf.parse(values[2]);
                end = sdf.parse(values[3]);

                runTime = runTime.plus(runWithThread(()->runGridDBQuery(sensorNames, start, end)));
                numQueries++;
                queryRunTimes.add(runTime);
                System.out.println(String.format("GridDB Count: %s Time: %s", numQueries, runTime.toString()));
            }
        } catch (Exception | Error e) {
            e.printStackTrace();
            queryRunTimes.add(Constants.MAX_DURATION);
        }

        // --------------------------------------------------------------------------

        System.out.println("Running PgSQL Big Join");
        numQueries = 0;
        queryRunTimes = new ArrayList<>();
        reader = new QueryCSVReader(queriesDir + getFileFromQueryWithVersion(12, selectivity));
        try {

            while ((values = reader.readNextLine()) != null) {
                String userId = values[1];
                Date start, end;
                start = sdf.parse(values[2]);
                end = sdf.parse(values[3]);
                runTime = runWithThread(()->runPGBigJoinQuery(userId, start, end));
                numQueries++;
                queryRunTimes.add(runTime);
                System.out.println(String.format("Postgres Count: %s Time: %s", numQueries, runTime.toString()));
            }
        } catch (Exception | Error e) {
            e.printStackTrace();
            queryRunTimes.add(Constants.MAX_DURATION);
        }

        System.out.println("Running GridDB Big App Join");
        numQueries = 0;
        reader = new QueryCSVReader(queriesDir + getFileFromQueryWithVersion(12, selectivity));
        try {
            while ((values = reader.readNextLine()) != null) {
                String userId = values[1];
                Date start, end;
                start = sdf.parse(values[2]);
                end = sdf.parse(values[3]);

                runTime = runTime.plus(runWithThread(()->runGridDBBigJoinQuery(userId, start, end)));
                numQueries++;
                queryRunTimes.add(runTime);
                System.out.println(String.format("GridDB Count: %s Time: %s", numQueries, runTime.toString()));
            }
        } catch (Exception | Error e) {
            e.printStackTrace();
            queryRunTimes.add(Constants.MAX_DURATION);
        }

        System.out.println("Running PgSQL Big App Join");
        numQueries = 0;
        queryRunTimes = new ArrayList<>();
        reader = new QueryCSVReader(queriesDir + getFileFromQueryWithVersion(12, selectivity));
        try {

            while ((values = reader.readNextLine()) != null) {
                String userId = values[1];
                Date start, end;
                start = sdf.parse(values[2]);
                end = sdf.parse(values[3]);
                runTime = runWithThread(()->runPGBigAppJoinQuery(userId, start, end));
                numQueries++;
                queryRunTimes.add(runTime);
                System.out.println(String.format("Postgres Count: %s Time: %s", numQueries, runTime.toString()));
            }
        } catch (Exception | Error e) {
            e.printStackTrace();
            queryRunTimes.add(Constants.MAX_DURATION);
        }

//        System.out.println("Removing Data And Schema");
//        dropGridDBSchmea();
//        dropPgSchema();

        return queryRunTimes;
    }

    private Configuration readConfiguration() throws BenchmarkException {
        try {
            Ini ini = new Ini(new File(getClass().getClassLoader().getResource(Constants.CONFIG).getFile()));
            Preferences prefs = new IniPreferences(ini);
            configuration = new Configuration(prefs);

        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error reading Configuration File");
        }
        return configuration;
    }


    public static void main(String args[]) {
        JoinPerformance exp = new JoinPerformance();
        for(int i=1; i<5; i++) {
            List<Duration> results = exp.runExperiment(i);
            LOGGER.info(results);
            System.out.print(results);
        }
    }

}
