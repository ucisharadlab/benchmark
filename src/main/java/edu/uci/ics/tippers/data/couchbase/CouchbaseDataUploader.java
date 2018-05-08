package edu.uci.ics.tippers.data.couchbase;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.common.util.Converter;
import edu.uci.ics.tippers.connection.couchbase.CouchbaseConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.model.observation.Observation;
import edu.uci.ics.tippers.model.platform.Platform;
import edu.uci.ics.tippers.model.semanticObservation.SemanticObservation;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;

public class CouchbaseDataUploader extends BaseDataUploader {

    private static final Logger LOGGER = Logger.getLogger(CouchbaseDataUploader.class);

    private CouchbaseConnectionManager connectionManager;
    private static final String QUERY_FORMAT = "INSERT INTO %s(%s)";
    private static String datePattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static SimpleDateFormat sdf = new SimpleDateFormat(datePattern);

    public CouchbaseDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
        connectionManager = CouchbaseConnectionManager.getInstance();
    }

    @Override
    public Database getDatabase() {
        return Database.COUCHBASE;
    }

    @Override
    public Duration addAllData() throws BenchmarkException {
        Instant start = Instant.now();

        addInfrastructureData();
        addUserData();
        addDeviceData();
        addSensorData();
        addObservationData();
        virtualSensorData();
        addSemanticObservationData();

        Instant end = Instant.now();
        return Duration.between(start, end);
    }

    private void simpleObservationInsert(String dataset, DataFiles dataFile) {
        switch (mapping) {
            case 1:
                BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + dataFile.getPath(),
                        Observation.class);
                Gson gson = new GsonBuilder()
                        .registerTypeAdapter(JSONObject.class, Converter.<JSONObject>getJSONSerializer())
                        .create();
                Observation obs;
                while ((obs = reader.readNext()) != null) {
                    JSONObject docToInsert = new JSONObject(gson.toJson(obs, Observation.class));
                    docToInsert.put("timeStamp", String.format("datetime('%s')", sdf.format(obs.getTimeStamp())));
                    String docString = docToInsert.toString().replaceAll("\"(datetime\\(.*\\))\"", "$1");
                    connectionManager.sendQuery(String.format(QUERY_FORMAT, "Observation", docString));
                }
                break;
            case 2:
                // TODO: Fast Insertion Code
                reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
                        Observation.class);
                gson = new GsonBuilder()
                        .registerTypeAdapter(JSONObject.class, Converter.<JSONObject>getJSONSerializer())
                        .create();
                while ((obs = reader.readNext()) != null) {
                    JSONObject docToInsert = new JSONObject(gson.toJson(obs, Observation.class));
                    docToInsert.put("sensorId", obs.getSensor().getId());
                    docToInsert.remove("sensor");
                    docToInsert.put("timeStamp", String.format("datetime('%s')", sdf.format(obs.getTimeStamp())));
                    String docString = docToInsert.toString().replaceAll("\"(datetime\\(.*\\))\"", "$1");
                    connectionManager.sendQuery(String.format(QUERY_FORMAT, "Observation", docString));
                }
                break;
        }
    }

    private void simpleSemanticObservationInsert(String dataset, DataFiles dataFile) {
        switch (mapping) {
            case 1:
                BigJsonReader<SemanticObservation> reader = new BigJsonReader<>(dataDir +dataFile.getPath(),
                        SemanticObservation.class);
                Gson gson = new GsonBuilder()
                        .registerTypeAdapter(JSONObject.class, Converter.<JSONObject>getJSONSerializer())
                        .create();
                SemanticObservation obs;
                while ((obs = reader.readNext()) != null) {
                    JSONObject docToInsert = new JSONObject(gson.toJson(obs, Observation.class));
                    docToInsert.put("timeStamp", String.format("datetime('%s')", sdf.format(obs.getTimeStamp())));
                    String docString = docToInsert.toString().replaceAll("\"(datetime\\(.*\\))\"", "$1");
                    connectionManager.sendQuery(String.format(QUERY_FORMAT, "SemanticObservation", docString));
                }
                break;
            case 2:
                // TODO: Fast Insertion Code
                reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
                        SemanticObservation.class);
                gson = new GsonBuilder()
                        .registerTypeAdapter(JSONObject.class, Converter.<JSONObject>getJSONSerializer())
                        .create();
                while ((obs = reader.readNext()) != null) {
                    JSONObject docToInsert = new JSONObject(gson.toJson(obs, Observation.class));
//                    docToInsert.put("sensorId", obs.getSensor().getId());
//                    docToInsert.remove("sensor");
                    docToInsert.put("timeStamp", String.format("datetime('%s')", sdf.format(obs.getTimeStamp())));
                    String docString = docToInsert.toString().replaceAll("\"(datetime\\(.*\\))\"", "$1");
                    connectionManager.sendQuery(String.format(QUERY_FORMAT, "SemanticObservation", docString));
                }
                break;
        }
    }

    private String prepareInsertQuery(String dataset, DataFiles dataFile) throws BenchmarkException {

        String values = null;
        try {
            values = new String(Files.readAllBytes(Paths.get(dataDir + dataFile.getPath())), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Reading Data Files");
        }

        return String.format(QUERY_FORMAT, dataset, values);
    }

    @Override
    public void addInfrastructureData() throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                connectionManager.sendQuery(prepareInsertQuery("InfrastructureType", DataFiles.INFRA_TYPE));
                connectionManager.sendQuery(prepareInsertQuery("Location", DataFiles.LOCATION));
                connectionManager.sendQuery(prepareInsertQuery("Infrastructure", DataFiles.INFRA));
                break;
        }
    }

    @Override
    public void addUserData() throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                connectionManager.sendQuery(prepareInsertQuery("UserGroup", DataFiles.GROUP));
                connectionManager.sendQuery(prepareInsertQuery("User", DataFiles.USER));
                break;
        }
    }

    @Override
    public void addSensorData() throws BenchmarkException {
        switch (mapping) {
            case 1:
                connectionManager.sendQuery(prepareInsertQuery("SensorType", DataFiles.SENSOR_TYPE));
                connectionManager.sendQuery(prepareInsertQuery("Sensor", DataFiles.SENSOR));
                break;
            case 2:
                connectionManager.sendQuery(prepareInsertQuery("SensorType", DataFiles.SENSOR_TYPE));
                String values = null;
                try {
                    values = new String(Files.readAllBytes(Paths.get(dataDir + DataFiles.SENSOR.getPath())),
                            StandardCharsets.UTF_8);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Reading Data Files");
                }
                JSONArray jsonArray = new JSONArray(values);
                jsonArray.forEach(e-> {
                    JSONObject docToInsert = (JSONObject)e;
                    docToInsert.put("infrastructureId", docToInsert.getJSONObject("infrastructure").getString("id"));
                    docToInsert.remove("infrastructure");
                    docToInsert.put("ownerId", docToInsert.getJSONObject("owner").getString("id"));
                    docToInsert.remove("infrastructure");

                    JSONArray entities = docToInsert.getJSONArray("coverage");
                    JSONArray entityIds = new JSONArray();
                    entities.forEach(entity->entityIds.put(((JSONObject)entity).getString("id")));
                    docToInsert.put("coverage", entityIds);

                    String docString = e.toString();
                    connectionManager.sendQuery(String.format(QUERY_FORMAT, "Sensor", docString));
                });
                break;
        }
    }

    @Override
    public void addDeviceData() throws BenchmarkException {
        switch (mapping) {
            case 1:
                connectionManager.sendQuery(prepareInsertQuery("PlatformType", DataFiles.PLT_TYPE));
                connectionManager.sendQuery(prepareInsertQuery("Platform", DataFiles.PLT));
                break;
            case 2:
                connectionManager.sendQuery(prepareInsertQuery("PlatformType", DataFiles.PLT_TYPE));
                String values = null;
                try {
                    values = new String(Files.readAllBytes(Paths.get(dataDir + DataFiles.PLT.getPath())),
                            StandardCharsets.UTF_8);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Reading Data Files");
                }
                JSONArray jsonArray = new JSONArray(values);
                jsonArray.forEach(e-> {
                    JSONObject docToInsert = (JSONObject)e;
                    docToInsert.put("ownerId", docToInsert.getJSONObject("owner").getString("id"));
                    docToInsert.remove("owner");
                    String docString = e.toString();
                    connectionManager.sendQuery(String.format(QUERY_FORMAT, "Platform", docString));
                });
                break;
        }
    }

    @Override
    public void addObservationData() throws BenchmarkException {

        LOGGER.info("Observation Loading Started");
        simpleObservationInsert("Observation", DataFiles.OBS);
        LOGGER.info("Observation Loading Complete");
    }

    @Override
    public void virtualSensorData() {
        switch (mapping) {
            case 1:
            case 2:
                connectionManager.sendQuery(prepareInsertQuery("SemanticObservationType", DataFiles.SO_TYPE));
                connectionManager.sendQuery(prepareInsertQuery("VirtualSensorType", DataFiles.VS_TYPE));
                connectionManager.sendQuery(prepareInsertQuery("VirtualSensor", DataFiles.VS));
                break;
        }
    }

    @Override
    public void addSemanticObservationData() {
        LOGGER.info("Semantic Observation Loading Started");
        simpleSemanticObservationInsert("SemanticObservation", DataFiles.SO);
        LOGGER.info("Semantic Observation Loading Complete");

    }

    @Override
    public Duration insertPerformance() throws BenchmarkException {
        Instant start = Instant.now();
        simpleObservationInsert("Observation", DataFiles.INSERT_TEST);
        Instant end = Instant.now();
        return Duration.between(start, end);
    }
}
