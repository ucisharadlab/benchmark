package edu.uci.ics.tippers.data.asterixdb;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.common.util.Converter;
import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.model.observation.Observation;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;

public class AsterixDBDataUploader extends BaseDataUploader {

    private AsterixDBConnectionManager connectionManager;
    private static final String QUERY_FORMAT = "INSERT INTO %s(%s)";
    private static String datePattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static SimpleDateFormat sdf = new SimpleDateFormat(datePattern);

    public AsterixDBDataUploader(int mapping, String dataDir) {
        super(mapping, dataDir);
        connectionManager = AsterixDBConnectionManager.getInstance();
    }

    @Override
    public Database getDatabase() {
        return Database.ASTERIXDB;
    }

    @Override
    public Duration addAllData() throws BenchmarkException {
        Instant start = Instant.now();

        addInfrastructureData();
        addUserData();
        addDeviceData();
        addSensorData();
        addObservationData();

        Instant end = Instant.now();
        return Duration.between(start, end);
    }

    public String prepareInsertQuery(String dataset, DataFiles dataFile) throws BenchmarkException {
        // TODO: Modify Observation Insertion Code
        if (dataFile == DataFiles.OBS)
            return String.format("LOAD DATASET %s USING localfs((\"path\"=\"%s://%s\"),(\"format\"=\"adm\"))",
                "Observation", "localhost", dataDir + dataFile.getPath());

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
                connectionManager.sendQuery(prepareInsertQuery("InfrastructureType", DataFiles.INFRA_TYPE));
                connectionManager.sendQuery(prepareInsertQuery("Location", DataFiles.LOCATION));
                connectionManager.sendQuery(prepareInsertQuery("Region", DataFiles.REGION));
                connectionManager.sendQuery(prepareInsertQuery("Infrastructure", DataFiles.INFRA));
                break;
        }
    }

    @Override
    public void addUserData() throws BenchmarkException {
        switch (mapping) {
            case 1:
                connectionManager.sendQuery(prepareInsertQuery("UserGroup", DataFiles.GROUP));
                connectionManager.sendQuery(prepareInsertQuery("User", DataFiles.USER));
                break;
        }
    }

    @Override
    public void addSensorData() throws BenchmarkException {
        switch (mapping) {
            case 1:
                connectionManager.sendQuery(prepareInsertQuery("ObservationType", DataFiles.OBS_TYPE));
                connectionManager.sendQuery(prepareInsertQuery("SensorType", DataFiles.SENSOR_TYPE));
                connectionManager.sendQuery(prepareInsertQuery("Sensor", DataFiles.SENSOR));
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
        }
    }

    @Override
    public void addObservationData() throws BenchmarkException {
        switch (mapping) {
            case 1:
                BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.OBS.getPath(),
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
        }
    }

    @Override
    public void virtualSensorData() {
        // TODO: Yet To Implement
    }

    @Override
    public void addSemanticObservationData() {
        // TODO: Yet To Implement
    }

}
