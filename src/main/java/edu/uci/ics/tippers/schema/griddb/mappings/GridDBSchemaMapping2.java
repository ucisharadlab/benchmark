package edu.uci.ics.tippers.schema.griddb.mappings;

import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.schema.griddb.GridDBBaseSchemaMapping;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;


public class GridDBSchemaMapping2 extends GridDBBaseSchemaMapping {

    private JSONParser parser = new JSONParser();
    private EnumSet<IndexType> indexSet = EnumSet.of(IndexType.HASH);
    private EnumSet<IndexType> treeIndex = EnumSet.of(IndexType.TREE);

    public GridDBSchemaMapping2(GridStore gridStore, String dataDir) {
        super(gridStore, dataDir);
    }

    public void createAll() throws GSException {

        createMetadataSchema();
        createDeviceSchema();
        createSensorAndObservationSchema();
        createVSensorAndSObservationSchema();

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
        gridStore.dropCollection("WiFiAPObservation");
        gridStore.dropCollection("WeMoObservation");
        gridStore.dropCollection("ThermometerObservation");

        gridStore.dropCollection("SemanticObservationType");
        gridStore.dropCollection("VirtualSensorType");
        gridStore.dropCollection("VirtualSensor");

        gridStore.dropCollection("Presence");
        gridStore.dropCollection("Occupancy");

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

        // Creating Observation Containers, One per each type
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("WiFiAPObservation");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("timeStamp", GSType.TIMESTAMP, treeIndex));
        columnInfoList.add(new ColumnInfo("sensorId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("clientId", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("WiFiAPObservation", containerInfo, true);

        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("WeMoObservation");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("timeStamp", GSType.TIMESTAMP, treeIndex));
        columnInfoList.add(new ColumnInfo("sensorId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("currentMilliWatts", GSType.INTEGER));
        columnInfoList.add(new ColumnInfo("onTodaySeconds", GSType.INTEGER));


        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("WeMoObservation", containerInfo, true);

        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("ThermometerObservation");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("timeStamp", GSType.TIMESTAMP, treeIndex));
        columnInfoList.add(new ColumnInfo("sensorId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("temperature", GSType.INTEGER));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("ThermometerObservation", containerInfo, true);


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
        columnInfoList.add(new ColumnInfo("language", GSType.STRING));
        columnInfoList.add(new ColumnInfo("projectName", GSType.STRING));
        columnInfoList.add(new ColumnInfo("typeId", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("VirtualSensor", containerInfo, true);

        // Creating Presence And Occupancy Containers
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("Presence");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("timeStamp", GSType.TIMESTAMP, treeIndex));
        columnInfoList.add(new ColumnInfo("virtualSensorId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("location", GSType.STRING));
        columnInfoList.add(new ColumnInfo("semanticEntityId", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("Presence", containerInfo, true);

        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("Occupancy");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("timeStamp", GSType.TIMESTAMP, treeIndex));
        columnInfoList.add(new ColumnInfo("virtualSensorId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("occupancy", GSType.INTEGER));
        columnInfoList.add(new ColumnInfo("semanticEntityId", GSType.STRING));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("Occupancy", containerInfo, true);

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
