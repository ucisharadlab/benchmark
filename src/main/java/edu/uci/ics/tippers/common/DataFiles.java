package edu.uci.ics.tippers.common;

public enum DataFiles {

    INFRA_TYPE ("infrastructureType.json"),
    LOCATION ("location.json"),
    REGION ("region.json"),
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
        return path;
    }

}
