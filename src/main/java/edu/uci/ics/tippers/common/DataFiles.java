package edu.uci.ics.tippers.common;

import java.util.HashMap;
import java.util.Map;

public enum DataFiles {

    INFRA_TYPE ("infrastructureType.json"),
    INFRA ("infrastructure.json") ,
    GROUP ("group.json"),
    USER ("user.json"),
    PLT_TYPE ("platformType.json"),
    PLT ("platform.json"),
    SENSOR_TYPE ("sensorType.json"),
    SENSOR ("sensor.json") ,
    OBS_TYPE ("observationType.json"),
    OBS ("observation.json"),
    VS_TYPE ("virtualSensorType.json"),
    VS ("virtualSensor.json"),
    SO_TYPE ("semanticObservationType.json"),
    SO ("semanticObservation.json");

    private final String path;

    private DataFiles(String path) {
        this.path = path;
    }

    public String getPath() {
        return "/data/" + path;
    }

}
