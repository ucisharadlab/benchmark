package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.exception.BenchmarkException;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

/* Class fetching the size of the Dataset being benchmarked
* */
public class DataSize {

    private String dataDir;
    private JSONParser parser = new JSONParser();

    public DataSize(String dataDir) {
        this.dataDir = dataDir;
    }

    public void appendInfoToFile(Writer writer) throws IOException {
        writer.write("---------------Dataset Size---------------\n\n");
        writer.write(String.format("Number Of Groups: %s\n", getNumberOfGroups()));
        writer.write(String.format("Number Of Users: %s\n", getNumberOfUsers()));
        writer.write(String.format("Number Of SensorTypes: %s\n", getNumberOfSensorTypes()));
        writer.write(String.format("Number Of Sensors: %s\n", getNumberOfSensors()));
        writer.write(String.format("Number Of InfraTypes: %s\n", getNumberOfInfraTypes()));
        writer.write(String.format("Number Of Infra: %s\n", getNumberOfInfra()));
        writer.write(String.format("Number Of Observations: %s\n", getNumberOfObservations()));
        writer.write("------------------------------------------\n\n");

    }

    private int countObjects(DataFiles dataFile) throws BenchmarkException {
        try {
            return ((JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + dataFile.getPath())))).size();

        } catch (ParseException | IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Counting Objects");
        }
    }

    public int getNumberOfGroups() {
        return countObjects(DataFiles.GROUP);
    }

    public int getNumberOfUsers() {
        return countObjects(DataFiles.USER);

    }

    public int getNumberOfSensors() {
        return countObjects(DataFiles.SENSOR);
    }

    public int getNumberOfSensorTypes() {
        return countObjects(DataFiles.SENSOR_TYPE);
    }

    public int getNumberOfInfraTypes() {
        return countObjects(DataFiles.INFRA_TYPE);
    }

    public int getNumberOfInfra() {
        return countObjects(DataFiles.INFRA);
    }

    public int getNumberOfObservations() {
        return countObjects(DataFiles.OBS);
    }

}
