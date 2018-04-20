package edu.uci.ics.tippers.query.jana;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.Helper;
import edu.uci.ics.tippers.connection.jana.JanaConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.query.asterixdb.AsterixDBQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JanaQueryManager extends BaseQueryManager {

    private static final Logger LOGGER = Logger.getLogger(AsterixDBQueryManager.class);
    private JanaConnectionManager connectionManager;

    private String senLevelDataDir="/home/sumaya/Desktop/benchmark/src/main/java/edu/uci/ics/tippers/data/jana/sensitivity_levels_data";
    private List<String> senSensorsList = getSensitiveSensors();
    private List<String> senUsersList = getSensitiveUsers();
    public JanaQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        connectionManager = JanaConnectionManager.getInstance();
    }

    @Override
    public Database getDatabase() {
        return Database.JANA;
    }

    @Override
    public void cleanUp() {

    }

    private Duration runTimedQuery (String query, int queryNum) throws BenchmarkException {

        LOGGER.info(String.format("Running Query %s", queryNum));
        LOGGER.info(query);

        Instant startTime = Instant.now();
        HttpResponse response = connectionManager.sendQuery(query);
        Instant endTime = Instant.now();


        if (writeOutput) {
            // TODO: Write To File
            try {
                RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping,
                        Helper.getFileFromQuery(queryNum));
                writer.writeString(EntityUtils.toString(response.getEntity()));
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new BenchmarkException("Error writing output to file");
            }
        }
        return Duration.between(startTime, endTime);
    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT NAME FROM SENSOR WHERE ID = '%s';", sensorId), 1
                );
            case 2:
                return runTimedQuery(
                        String.format("SELECT NAME FROM SENSOR WHERE ID = '%s';", sensorId), 1
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT timeStamp, clientId FROM WiFiAPObservation WHERE sensor_id='%s' "
                                        + "AND timeStamp >=%s AND timeStamp <=%s;",
                                sensorId, startTime.getTime()/1000, endTime.getTime()/1000), 3
                );
            case 2://partition the query based on whether the data reside on the S or NS table
                if(senSensorsList.contains(sensorId))
                    return runTimedQuery(String.format("SELECT timeStamp, clientId FROM WiFiAPObservation_S WHERE sensor_id='%s' AND timeStamp >=%s AND timeStamp <=%s;",
                        sensorId, startTime.getTime()/1000,endTime.getTime()/1000), 3);
                else
                    return runTimedQuery(String.format("SELECT timeStamp, clientId FROM WiFiAPObservation_NS WHERE sensor_id='%s' "
                                + "AND timeStamp >=%s AND timeStamp <=%s;",
                        sensorId, startTime.getTime()/1000, endTime.getTime()/1000),3);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute, Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery8(String userId, Date date) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery9(String userId, String infraTypeName) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery10(Date startTime, Date endTime) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }

    @Override
    public Duration runQuery11() throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(String.format("Select p.USER_ID,count(*) from PLATFORM p join WiFiAPObservation w ON w.clientId=p.ID GROUP BY p.USER_ID"), 11);
            case 2://partition the query based on whether the data reside on the S or NS table
                    Instant startTime = Instant.now();
                    runTimedQuery(String.format("Select p.USER_ID,count(*) from PLATFORM p join WiFiAPObservation_S w ON w.clientId=p.HASHED_MAC GROUP BY p.USER_ID"), 11);
                    runTimedQuery(String.format("Select p.USER_ID,count(*) from PLATFORM p join WiFiAPObservation_NS w ON w.clientId=p.ID GROUP BY p.USER_ID"), 11);
                    Instant endTime = Instant.now();
                return Duration.between(startTime, endTime);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    //
    private List<String> getSensitiveSensors(){
        List<String> senSpaceList = new ArrayList<String>();
        //String fileName = senLevelDataDir+"/small_dataset/sensors_low";
        //String fileName = senLevelDataDir+"/small_dataset/sensors_medium";
        //String fileName = senLevelDataDir+"/small_dataset//sensors_high";
        String fileName = senLevelDataDir+"/small_dataset//sensors_full";
        String line;

        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line=bufferedReader.readLine())!= null)
                senSpaceList.add(line);

            bufferedReader.close();
        }
        catch(FileNotFoundException ex){
            System.out.println(ex.getMessage());
        }
        catch(IOException ex) {
            System.out.println(ex.getMessage());
        }

        return senSpaceList;
    }

    private List<String> getSensitiveUsers(){
        List<String> senUsersList = new ArrayList<String>();
        //String fileName = senLevelDataDir+"/small_dataset/users_low";
        //String fileName = senLevelDataDir+"/small_dataset//users_medium";
        //String fileName = senLevelDataDir+"/small_dataset//users_high";
        String fileName = senLevelDataDir+"/small_dataset//users_full";
        String line;

        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line=bufferedReader.readLine())!= null)
                senUsersList.add(line);

            bufferedReader.close();
        }
        catch(FileNotFoundException ex){
            System.out.println(ex.getMessage());
        }
        catch(IOException ex) {
            System.out.println(ex.getMessage());
        }

        return senUsersList;
    }
}
