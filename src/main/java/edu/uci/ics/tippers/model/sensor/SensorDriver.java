package edu.uci.ics.tippers.model.sensor;

/**
 * Created by peeyush on 23/2/17.
 */
public class SensorDriver {

    private String id;

    private String name;

    private String description;

    private String sourceFileLocation;

    private String compiledCodeLocation;

    private String language;

    private String projectName;

    private String compileDirectory;

    private String executeDirectory;

    private String compileCommand;

    private String executeCommand;

    private SensorType SensorType;

    public SensorDriver(){
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getSourceFileLocation() {
        return sourceFileLocation;
    }

    public void setSourceFileLocation(String sourceFileLocation) {
        this.sourceFileLocation = sourceFileLocation;
    }

    public String getCompiledCodeLocation() {
        return compiledCodeLocation;
    }

    public void setCompiledCodeLocation(String compiledCodeLocation) {
        this.compiledCodeLocation = compiledCodeLocation;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getCompileDirectory() {
        return compileDirectory;
    }

    public void setCompileDirectory(String compileDirectory) {
        this.compileDirectory = compileDirectory;
    }

    public String getExecuteDirectory() {
        return executeDirectory;
    }

    public void setExecuteDirectory(String executeDirectory) {
        this.executeDirectory = executeDirectory;
    }

    public String getCompileCommand() {
        return compileCommand;
    }

    public void setCompileCommand(String compileCommand) {
        this.compileCommand = compileCommand;
    }

    public String getExecuteCommand() {
        return executeCommand;
    }

    public void setExecuteCommand(String executeCommand) {
        this.executeCommand = executeCommand;
    }

    public SensorType getSensorType() {
        return SensorType;
    }

    public void setSensorType(SensorType sensorType) {
        SensorType = sensorType;
    }
}
