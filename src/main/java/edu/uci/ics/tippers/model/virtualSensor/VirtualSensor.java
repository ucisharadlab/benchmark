package edu.uci.ics.tippers.model.virtualSensor;

public class VirtualSensor {
	
	public VirtualSensor() {
		super();
	}

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

	private VirtualSensorConfig config;
	
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

	public VirtualSensorType getType() {
		return type;
	}

	public void setType(VirtualSensorType type) {
		this.type = type;
	}

	private VirtualSensorType type;

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

	public VirtualSensorConfig getConfig() {
		return config;
	}

	public void setConfig(VirtualSensorConfig config) {
		this.config = config;
	}

}
