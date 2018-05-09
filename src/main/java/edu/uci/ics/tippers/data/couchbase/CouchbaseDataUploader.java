package edu.uci.ics.tippers.data.couchbase;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.common.util.Converter;
import edu.uci.ics.tippers.connection.couchbase.CouchbaseConnectionManager;
import edu.uci.ics.tippers.connection.influxdb.InfluxDBConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.model.observation.Observation;
import edu.uci.ics.tippers.model.platform.Platform;
import edu.uci.ics.tippers.model.semanticObservation.SemanticObservation;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CouchbaseDataUploader extends BaseDataUploader {

    private static final Logger LOGGER = Logger.getLogger(CouchbaseDataUploader.class);

    private CouchbaseConnectionManager connectionManager;
    private static final String QUERY_FORMAT = "INSERT INTO %s VALUES(%s, %s)";
    private static String datePattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static SimpleDateFormat sdf = new SimpleDateFormat(datePattern);
    private JSONParser parser = new JSONParser();

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

//        addInfrastructureData();
//        addUserData();
//        addDeviceData();
//        addSensorData();
//        addObservationData();
//        virtualSensorData();
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

                List<String> batch = new ArrayList<>();
                int count = 1;
                while ((obs = reader.readNext()) != null) {
                    JSONObject docToInsert = new JSONObject(gson.toJson(obs, Observation.class));
                    docToInsert.put("timeStamp",  sdf.format(obs.getTimeStamp()));
                    batch.add(String.format("VALUES (%s, %s)",
                            "\"" + docToInsert.getString("id") +"\"",docToInsert.toString()));

                    if (count % Constants.COUCHBASE_BATCH_SIZE == 0) {
                        connectionManager.sendQuery( String.format("INSERT INTO %s %s", dataset,
                                batch.stream().collect(Collectors.joining(",\n"))));
                        batch = new ArrayList<>();
                    }

                    if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
                    count ++;
                }
                connectionManager.sendQuery( String.format("INSERT INTO %s %s", dataset,
                        batch.stream().collect(Collectors.joining(",\n"))));
                break;
            case 2:
                // TODO: Fast Insertion Code
                reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
                        Observation.class);
                gson = new GsonBuilder()
                        .registerTypeAdapter(JSONObject.class, Converter.<JSONObject>getJSONSerializer())
                        .create();

                batch = new ArrayList<>();
                count = 1;
                while ((obs = reader.readNext()) != null) {
                    JSONObject docToInsert = new JSONObject(gson.toJson(obs, Observation.class));
                    docToInsert.put("sensorId", obs.getSensor().getId());
                    docToInsert.remove("sensor");
                    docToInsert.put("timeStamp",  sdf.format(obs.getTimeStamp()));
                    batch.add(String.format("VALUES (%s, %s)",
                            "\"" + docToInsert.getString("id") +"\"",docToInsert.toString()));

                    if (count % Constants.COUCHBASE_BATCH_SIZE == 0) {
                        connectionManager.sendQuery( String.format("INSERT INTO %s %s", dataset,
                                batch.stream().collect(Collectors.joining(",\n"))));
                        batch = new ArrayList<>();
                    }

                    if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
                    count ++;
                }
                connectionManager.sendQuery( String.format("INSERT INTO %s %s", dataset,
                        batch.stream().collect(Collectors.joining(",\n"))));
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
                List<String> batch = new ArrayList<>();
                int count = 1;
                while ((obs = reader.readNext()) != null) {
                    JSONObject docToInsert = new JSONObject(gson.toJson(obs, SemanticObservation.class));
                    docToInsert.put("timeStamp",  sdf.format(obs.getTimeStamp()));
                    batch.add(String.format("VALUES (%s, %s)",
                            "\"" + docToInsert.getString("id") +"\"",docToInsert.toString()));

                    if (count % Constants.COUCHBASE_BATCH_SIZE == 0) {
                        connectionManager.sendQuery( String.format("INSERT INTO %s %s", dataset,
                                batch.stream().collect(Collectors.joining(",\n"))));
                        batch = new ArrayList<>();
                    }

                    if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
                    count ++;
                }
                connectionManager.sendQuery( String.format("INSERT INTO %s %s", dataset,
                        batch.stream().collect(Collectors.joining(",\n"))));
                break;
            case 2:
                // TODO: Fast Insertion Code
                reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
                        SemanticObservation.class);
                gson = new GsonBuilder()
                        .registerTypeAdapter(JSONObject.class, Converter.<JSONObject>getJSONSerializer())
                        .create();

                batch = new ArrayList<>();
                count = 1;
                while ((obs = reader.readNext()) != null) {
                    JSONObject docToInsert = new JSONObject(gson.toJson(obs, SemanticObservation.class));
                    docToInsert.put("virtualSensorId", obs.getVirtualSensor().getId());
                    docToInsert.remove("virtualSensor");
                    docToInsert.getJSONObject("semanticEntity").remove("geometry");
                    docToInsert.put("timeStamp",  sdf.format(obs.getTimeStamp()));
                    batch.add(String.format("VALUES (%s, %s)",
                            "\"" + docToInsert.getString("id") +"\"",docToInsert.toString()));

                    if (count % Constants.COUCHBASE_BATCH_SIZE == 0) {
                        connectionManager.sendQuery( String.format("INSERT INTO %s %s", dataset,
                                batch.stream().collect(Collectors.joining(",\n"))));
                        batch = new ArrayList<>();
                    }

                    if (count % Constants.LOG_LIM == 0) LOGGER.info(String.format("%s Observations", count));
                    count ++;
                }
                connectionManager.sendQuery( String.format("INSERT INTO %s %s", dataset,
                        batch.stream().collect(Collectors.joining(",\n"))));
                break;
        }
    }

    private void prepareInsertQuery(String dataset, DataFiles dataFile) throws BenchmarkException {

        String values = null;

        try {
            org.json.simple.JSONArray jsonArray = (org.json.simple.JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + dataFile.getPath())));
            for (Object item : jsonArray) {
                connectionManager.sendQuery(String.format(QUERY_FORMAT, dataset,
                        "\"" + ((org.json.simple.JSONObject) item).get("id") + "\"", item.toString()));
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Reading Data Files");
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void addInfrastructureData() throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                prepareInsertQuery("InfrastructureType", DataFiles.INFRA_TYPE);
                prepareInsertQuery("Location", DataFiles.LOCATION);
                prepareInsertQuery("Infrastructure", DataFiles.INFRA);
                break;
        }
    }

    @Override
    public void addUserData() throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                prepareInsertQuery("UserGroup", DataFiles.GROUP);
                prepareInsertQuery("Users", DataFiles.USER);
                break;
        }
    }

    @Override
    public void addSensorData() throws BenchmarkException {
        switch (mapping) {
            case 1:
                prepareInsertQuery("SensorType", DataFiles.SENSOR_TYPE);
                prepareInsertQuery("Sensor", DataFiles.SENSOR);
                break;
            case 2:
                prepareInsertQuery("SensorType", DataFiles.SENSOR_TYPE);
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
                    String.format(QUERY_FORMAT, "Sensor", docString);
                });
                break;
        }
    }

    @Override
    public void addDeviceData() throws BenchmarkException {
        switch (mapping) {
            case 1:
                prepareInsertQuery("PlatformType", DataFiles.PLT_TYPE);
                prepareInsertQuery("Platform", DataFiles.PLT);
                break;
            case 2:
                prepareInsertQuery("PlatformType", DataFiles.PLT_TYPE);
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
                    String.format(QUERY_FORMAT, "Platform", docString);
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
                prepareInsertQuery("SemanticObservationType", DataFiles.SO_TYPE);
                prepareInsertQuery("VirtualSensorType", DataFiles.VS_TYPE);
                prepareInsertQuery("VirtualSensor", DataFiles.VS);
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
