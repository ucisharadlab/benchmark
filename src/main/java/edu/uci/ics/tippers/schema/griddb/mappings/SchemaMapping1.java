package edu.uci.ics.tippers.schema.griddb.mappings;

import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.schema.griddb.BaseSchemaMapping;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Created by peeyush on 26/4/17.
 */
public class SchemaMapping1 extends BaseSchemaMapping {

    private GridStore gridStore;
    private EnumSet<IndexType> indexSet = EnumSet.of(IndexType.HASH);

    public SchemaMapping1(GridStore gridStore, String dataDir) {
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
        gridStore.dropCollection("Region");
        gridStore.dropCollection("InfrastructureType");
        gridStore.dropCollection("Infrastructure");

        gridStore.dropCollection("PlatformType");
        gridStore.dropCollection("Platform");
        gridStore.dropCollection("SensorType");
        gridStore.dropCollection("ObservationType");
        gridStore.dropCollection("SensorCoverage");

        gridStore.dropCollection("Sensor");

        // TODO: Dropping Observation Collection

        gridStore.dropCollection("SemanticObservationType");
        gridStore.dropCollection("VirtualSensorType");
        gridStore.dropCollection("VirtualSensor");

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
        columnInfoList.add(new ColumnInfo("locationId", GSType.STRING));
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
        columnInfoList.add(new ColumnInfo("captureFun", GSType.STRING));
        columnInfoList.add(new ColumnInfo("observationTypeId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("payloadSchema", GSType.STRING));


        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);
        gridStore.putCollection("SensorType", containerInfo, true);

        // Creating Observation Types
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("ObservationType");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("description", GSType.STRING));
        columnInfoList.add(new ColumnInfo("payloadSchema", GSType.STRING));


        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("ObservationType", containerInfo, true);

        // Creating Sensor  Coverage
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("SensorCoverage");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("radius", GSType.FLOAT));
        columnInfoList.add(new ColumnInfo("entitiesCovered", GSType.STRING_ARRAY));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("SensorCoverage", containerInfo, true);

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
        columnInfoList.add(new ColumnInfo("platformId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("coverageId", GSType.STRING));
        columnInfoList.add(new ColumnInfo("sensorConfig", GSType.STRING));


        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("Sensor", containerInfo, true);

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

        // Creating Regions
        containerInfo = new ContainerInfo();
        columnInfoList = new ArrayList<>();
        containerInfo.setName("Region");
        containerInfo.setType(ContainerType.COLLECTION);

        columnInfoList.add(new ColumnInfo("id", GSType.STRING, indexSet));
        columnInfoList.add(new ColumnInfo("floor", GSType.DOUBLE));
        columnInfoList.add(new ColumnInfo("name", GSType.STRING));
        columnInfoList.add(new ColumnInfo("geometry", GSType.STRING_ARRAY));

        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("Region", containerInfo, true);

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
        columnInfoList.add(new ColumnInfo("regionId", GSType.STRING));


        containerInfo.setColumnInfoList(columnInfoList);
        containerInfo.setRowKeyAssigned(true);

        gridStore.putCollection("Infrastructure", containerInfo, true);

    }
}
