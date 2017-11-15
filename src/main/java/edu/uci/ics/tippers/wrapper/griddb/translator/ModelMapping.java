package edu.uci.ics.tippers.wrapper.griddb.translator;

import edu.uci.ics.tippers.model.metadata.infrastructure.Infrastructure;
import edu.uci.ics.tippers.model.metadata.infrastructure.InfrastructureType;
import edu.uci.ics.tippers.model.metadata.infrastructure.Location;

import edu.uci.ics.tippers.model.metadata.user.User;
import edu.uci.ics.tippers.model.metadata.user.UserGroup;
import edu.uci.ics.tippers.model.observation.Observation;
import edu.uci.ics.tippers.model.platform.Platform;
import edu.uci.ics.tippers.model.platform.PlatformType;

import edu.uci.ics.tippers.model.semanticObservation.SemanticObservation;
import edu.uci.ics.tippers.model.semanticObservation.SemanticObservationType;
import edu.uci.ics.tippers.model.sensor.Sensor;
import edu.uci.ics.tippers.model.sensor.SensorType;
import edu.uci.ics.tippers.model.virtualSensor.VirtualSensor;
import edu.uci.ics.tippers.model.virtualSensor.VirtualSensorType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by peeyush on 26/6/17.
 */
public class ModelMapping {
    public static Map<Class, String> collectionModelMap = new HashMap<>();
    public static Map<Class, List<String>> foreignKeys = new HashMap<>();

    static {
        // Generics
        collectionModelMap.put(Location.class, "Location");
        collectionModelMap.put(UserGroup.class, "Group");
        collectionModelMap.put(User.class, "User");
        collectionModelMap.put(InfrastructureType.class, "InfrastructureType");
        collectionModelMap.put(PlatformType.class, "PlatformType");
        collectionModelMap.put(SemanticObservationType.class, "SemanticObservationType");

        // Non Generics
        collectionModelMap.put(Infrastructure.class, "Infrastructure");
        collectionModelMap.put(Platform.class, "Platform");
        collectionModelMap.put(SensorType.class, "SensorType");
        collectionModelMap.put(Sensor.class, "Sensor");
        collectionModelMap.put(VirtualSensorType.class, "VirtualSensorType");
        collectionModelMap.put(VirtualSensor.class, "VirtualSensor");
        collectionModelMap.put(Observation.class, "Observation");
        collectionModelMap.put(SemanticObservation.class, "SemanticObservation");

        // Non Generic Fields
        foreignKeys.put(Infrastructure.class, Arrays.asList("type", "region"));
        foreignKeys.put(Platform.class, Arrays.asList("geometry"));
        foreignKeys.put(SensorType.class, Arrays.asList("geometry"));
        foreignKeys.put(Sensor.class, Arrays.asList("geometry"));
        foreignKeys.put(VirtualSensorType.class, Arrays.asList("geometry"));
        foreignKeys.put(VirtualSensor.class, Arrays.asList("geometry"));
    }
}
