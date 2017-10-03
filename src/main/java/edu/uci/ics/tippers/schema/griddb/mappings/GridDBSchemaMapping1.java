package edu.uci.ics.tippers.schema.griddb.mappings;

import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.schema.griddb.GridDBBaseSchemaMapping;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class GridDBSchemaMapping1 extends GridDBBaseSchemaMapping {

    private JSONParser parser = new JSONParser();
    private EnumSet<IndexType> indexSet = EnumSet.of(IndexType.HASH);

    public GridDBSchemaMapping1(GridStore gridStore, String dataDir) {
        super(gridStore, dataDir);
    }

    public void createAll() throws GSException {

        createMetadataSchema();
        createDeviceSchema();
        createSensorAndObservationSchema();
        //createVSensorAndSObservationSchema();

    }

    public void deleteAll() throws GSException {

        gridStore.dropCollection("Group");
        gridStore.dropCollection("User");


        gridStore.dropCollection("Location");
        gridStore.dropCollection("InfrastructureType");
        gridStore.dropCollection("Infrastructure");

        gridStore.dropCollection("PlatformType");
        gridStore.dropCollection("Platform");
        gridStore.dropCollection("SensorType");

        gridStore.dropCollection("Sensor");

        // TODO: Dropping Observation Collection

//        gridStore.dropCollection("SemanticObservationType");
//        gridStore.dropCollection("VirtualSensorType");
//        gridStore.dropCollection("VirtualSensor");

    }


    public void createDeviceSchema() throws GSException {
        ContainerInfo containerInfo = new ContainerInfo();
        List<ColumnInfo> columnInfoList = new ArrayList<>();

        // Creating Platform Types
        containerInfo.setName("PlatformType");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("description", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("PlatformType", containerInfo, true);

        // Creating Platforms
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("Platform");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("hashedMac", GSType.STRING));
        columnInfoList.add(new ColumnInfo("ownerId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("typeId", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("Platform", containerInfo, true);
    }

    public void createSensorAndObservationSchema() throws GSException {
        ContainerInfo containerInfo = new ContainerInfo();
        List<ColumnInfo> columnInfoList = new ArrayList<>();

        // Creating Sensor Types
        containerInfo.setName("SensorType");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("description", GSType.STRING));
        columnInfoList.add(new ColumnInfo("mobility", GSType.STRING));
        columnInfoList.add(new ColumnInfo("captureFunctionality", GSType.STRING));
        columnInfoList.add(new ColumnInfo("payloadSchema", GSType.STRING));


        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);
        gridStore.putCollection("SensorType", containerInfo, true);

        // Creating Sensors
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("Sensor");
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

        gridStore.putCollection("Sensor", containerInfo, true);

        // Creating Time series Observation Containers, One per each Sensor
        JSONArray sensorList = null;
        try {
            sensorList = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SENSOR.getPath())));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        for(int i =0;i<sensorList.size();i++){
            JSONObject temp=(JSONObject)sensorList.get(i);

            containerInfo = new ContainerInfo();
            List<ColumnInfo> tempColumnInfoList = new ArrayList<>();
            String collectionName = Constants.GRIDDB_OBS_PREFIX + temp.get("id");
            containerInfo.setName(collectionName);
            containerInfo.setType(ContainerType.TIME_SERIES);

            tempColumnInfoList.add(new ColumnInfo("timeStamp", GSType.TIMESTAMP));
            tempColumnInfoList.add(new ColumnInfo("id", GSType.STRING));

            JSONArray schema = null;
            try {
                schema = (JSONArray) parser.parse((String)
                                ((JSONObject)temp.get("type_"))
                        .get("payloadSchema"));
            } catch (ParseException e) {
                e.printStackTrace();
            }

            Iterator<JSONObject> iterator = schema.iterator();

            iterator.forEachRemaining( e-> {
                Set<String> keys =  e.keySet();

                keys.forEach(k-> {
                    GSType type = null;
                    if (e.get(k).equals("STRING"))
                        type = GSType.STRING;
                    if (e.get(k).equals("DOUBLE"))
                        type = GSType.DOUBLE;
                    if (e.get(k).equals("INTEGER"))
                        type = GSType.INTEGER;

                    tempColumnInfoList.add(new ColumnInfo(k, type));
                });
            });

            containerInfo.setColumnInfoList(tempColumnInfoList);
            containerInfo.setRowKeyAssigned(true);

            gridStore.putTimeSeries(collectionName, containerInfo, true);
        }

    }

    public void createVSensorAndSObservationSchema() throws GSException {
        ContainerInfo containerInfo = new ContainerInfo();
        List<ColumnInfo> columnInfoList = new ArrayList<>();

        // Creating Semantic Observation Type
        containerInfo.setName("SemanticObservationType");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("description", GSType.STRING));
        columnInfoList.add(new ColumnInfo("payloadSchema", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("SemanticObservationType", containerInfo, true);

        // Creating Virtual Sensor Type
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("VirtualSensorType");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("description", GSType.STRING));
        columnInfoList.add(new ColumnInfo("inputTypeId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("semanticObservationTypeId", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("VirtualSensorType", containerInfo, true);


        // Creating Virtual Sensors
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("VirtualSensor");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("description", GSType.STRING));
        columnInfoList.add(new ColumnInfo("sourceFileLocation", GSType.STRING));
        columnInfoList.add(new ColumnInfo("compiledCodeLocation", GSType.STRING));
        columnInfoList.add(new ColumnInfo("language", GSType.STRING));
        columnInfoList.add(new ColumnInfo("projectName", GSType.STRING));
        columnInfoList.add(new ColumnInfo("compileDirectory", GSType.STRING));
        columnInfoList.add(new ColumnInfo("executeDirectory", GSType.STRING));
        columnInfoList.add(new ColumnInfo("compileCommand", GSType.STRING));
        columnInfoList.add(new ColumnInfo("executeCommand", GSType.STRING));
        columnInfoList.add(new ColumnInfo("typeId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("config", GSType.STRING));


        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("VirtualSensor", containerInfo, true);

        // Creating Semantic Observation Containers, One per each Semantic Observation Type
        JSONArray soTypeList = null;
        try {
            soTypeList = (JSONArray) parser.parse(new InputStreamReader(
                    new FileInputStream(dataDir + DataFiles.SO_TYPE.getPath())));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }


        for(int i =0;i<soTypeList.size();i++) {
            JSONObject temp=(JSONObject)soTypeList.get(i);
            String collectionName = Constants.GRIDDB_SO_PREFIX + temp.get("id");
            containerInfo = new ContainerInfo();
            List<ColumnInfo> tempColumnInfoList = new ArrayList<>();
            containerInfo.setName(collectionName);
            containerInfo.setType(ContainerType.COLLECTION);

            tempColumnInfoList.add(new ColumnInfo("id", GSType.STRING));
            tempColumnInfoList.add(new ColumnInfo("timeStamp", GSType.TIMESTAMP));
            tempColumnInfoList.add(new ColumnInfo("semanticEntityId", GSType.STRING));
            tempColumnInfoList.add(new ColumnInfo("virtualSensorId", GSType.STRING));


            JSONArray schema = null;
            try {
                schema = (JSONArray) parser.parse((String)temp.get("payloadSchema"));
            } catch (ParseException e) {
                e.printStackTrace();
            }

            Iterator<JSONObject> iterator = schema.iterator();

            iterator.forEachRemaining( e-> {
                Set<String> keys =  e.keySet();

                keys.forEach(k-> {
                    GSType type = null;
                    if (e.get(k).equals("STRING"))
                        type = GSType.STRING;
                    if (e.get(k).equals("DOUBLE"))
                        type = GSType.DOUBLE;
                    if (e.get(k).equals("INTEGER"))
                        type = GSType.INTEGER;

                    tempColumnInfoList.add(new ColumnInfo(k, type));
                });
            });
            containerInfo.setColumnInfoList(tempColumnInfoList);
            containerInfo.setRowKeyAssigned(true);

            gridStore.putCollection(collectionName, containerInfo, true);
        }
    }

    public void createMetadataSchema() throws GSException {

        ContainerInfo containerInfo = new ContainerInfo();
        List<ColumnInfo> columnInfoList = new ArrayList<>();

        // Creating Groups
        containerInfo.setName("Group");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("description", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("Group", containerInfo, true);

        // Creating Users
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("User");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("emailId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("googleAuthToken", GSType.STRING));
        columnInfoList.add(new ColumnInfo("groupIds", GSType.STRING_ARRAY));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("User", containerInfo, true);

        //Creating Locations
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("Location");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("x", GSType.DOUBLE));
        columnInfoList.add(new ColumnInfo("y", GSType.DOUBLE));
        columnInfoList.add(new ColumnInfo("z", GSType.DOUBLE));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("Location", containerInfo, true);

        // Creating Infrastructure Type
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("InfrastructureType");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("description", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("InfrastructureType", containerInfo, true);

        // Creating Infrastructure
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("Infrastructure");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("typeId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("floor", GSType.INTEGER));
        columnInfoList.add(new ColumnInfo("geometry", GSType.STRING_ARRAY));


        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("Infrastructure", containerInfo, true);

    }
}
