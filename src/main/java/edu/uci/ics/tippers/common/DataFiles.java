package edu.uci.ics.tippers.common;

public enum DataFiles {

    INFRA_TYPE ("infrastructureType"),
    LOCATION ("location"),
    REGION ("region"),
    INFRA ("infrastructure") ,
    GROUP ("group"),
    USER ("user"),
    PLT_TYPE ("platformType"),
    PLT ("platform"),
    SENSOR_TYPE ("sensorType"),
    SENSOR ("sensor") ,
    OBS ("observation"),
    VS_TYPE ("virtualSensorType"),
    VS ("virtualSensor"),
    SO_TYPE ("semanticObservationType"),
    SO ("semanticObservation"),
    INSERT_TEST ("insertTestData");

    private final String path;

    private DataFiles(String path) {
        this.path = path;
    }

    public String getPath() {
        return getPathByTypeAndMapping("json", 0);
    }
    
    public String getPathByTypeAndMapping(String type, int mapping) {
        if (mapping != 0)
            return String.format("%s.%s.%s", path, mapping, type);
        return String.format("%s.%s", path, type);
    }

}
