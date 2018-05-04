package edu.uci.ics.tippers.encryption;

import edu.uci.ics.tippers.common.Database;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DefaultSensitivityManager extends BaseSensitivityManager {

    private Database database;
    private int mapping;
    private List<String> sensitiveUsers;
    private List<String> sensitiveSensors;
    private String senLevelDataDir;
    private String SENSOR_FILE = "/medium_dataset/sensors_medium";
    private String USER_FILE = "/medium_dataset/users_medium";

    public DefaultSensitivityManager(Database database, int mapping, String senLevelDataDir) {
        super();
        this.database = database;
        this.mapping = mapping;
        this.senLevelDataDir = senLevelDataDir;
        this.sensitiveSensors = getSensitiveSensors();
        this.sensitiveUsers = getSensitiveUsers();
    }

    @Override
    public Boolean checkUserSensitive(String userId) {
        switch (database) {
            case JANA:
                return sensitiveUsers.contains(userId);
            default:
                return false;
        }
    }

    @Override
    public Boolean checkSensorSensitive(String sensorId) {
        switch (database) {
            case JANA:
                return sensitiveSensors.contains(sensorId);
            default:
                return false;
        }
    }

    @Override
    public Boolean checkObservationSensitive(JSONObject observation) {
        switch (database) {
            case JANA:
                return checkSensorSensitive(observation.get("sensorId").toString()) &&
                        checkUserSensitive(((JSONObject)observation.get("payload")).get("clientId").toString());
            default:
                return false;
        }
    }

    private List<String> getSensitiveUsers(){
        List<String> senUsersList = new ArrayList<String>();
        String fileName = senLevelDataDir+SENSOR_FILE;
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

    private List<String> getSensitiveSensors(){
        List<String> senSpaceList = new ArrayList<String>();
        String fileName = senLevelDataDir+USER_FILE;
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


}
