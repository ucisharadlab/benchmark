package edu.uci.ics.tippers.scaler.data;

import edu.uci.ics.tippers.common.constants.Constants;
import org.ini4j.Ini;
import org.ini4j.IniPreferences;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.prefs.Preferences;

// Read and parse configure.txt file
public class DataConfiguration {

	private int timeScaleNum;
	private int speedScaleNum;
	private int deviceScaleNum;
	private double timeScaleNoise;
	private double speedScaleNoise;
	private double deviceScaleNoise;
	private String inputFilename;
	private String outputFilename;
	private String sensorType;
	private int personNum;
	private int timeInterval; // mins
	private String dataDir;

	// Constructor
	public DataConfiguration(String dataDir) throws IOException {
		this.dataDir = dataDir;
		parseFile();
	}

	// Read configure.txt file and extract scale number
	public void parseFile() throws IOException {

		Ini ini = new Ini(new File(getClass().getClassLoader().getResource(Constants.CONFIG).getFile()));
		Preferences prefs = new IniPreferences(ini);

		sensorType = prefs.node("scaledata").get("write-query-result", null);

		timeScaleNum = prefs.node("scaledata").getInt("timeScaleNum", 0);
		speedScaleNum = prefs.node("scaledata").getInt("speedScaleNum", 1);
		deviceScaleNum = prefs.node("scaledata").getInt("deviceScaleNum", 1);
		timeScaleNoise = prefs.node("scaledata").getInt("timeScaleNoise", 0);
		speedScaleNoise = prefs.node("scaledata").getInt("speedScaleNoise", 0);
		deviceScaleNoise = prefs.node("scaledata").getInt("deviceScaleNoise", 0);

		inputFilename = prefs.node("scaledata").get("inputFilename", null);
		outputFilename = prefs.node("scaledata").get("outputFilename", null);

		personNum= prefs.node("scaledata").getInt("personNum", 0);
		timeInterval= prefs.node("scaledata").getInt("timeInterval", 0);

	}

	// Return sensorType: int, double, or trajectory
	public String getSensorType() {
		return this.sensorType;
	}

	// Return time scale number: number of days extended based on original days
	public int getTimeScaleNum() {
		return this.timeScaleNum;
	}

	// Return speed scale number: times of original observation number
	public int getSpeedScaleNum() {
		return this.speedScaleNum;
	}

	// Return device scale number: times of original device number
	public int getDeviceScaleNum() {
		return this.deviceScaleNum;
	}

	// Return time scale noise: the percentage
	public double getTimeScaleNoise() {
		return this.timeScaleNoise;
	}

	// Return speed scale noise: the percentage
	public double getSpeedScaleNoise() {
		return this.speedScaleNoise;
	}

	// Return device scale noise: the percentage
	public double getDeviceScaleNoise() {
		return this.deviceScaleNoise;
	}

	// Return the input filename
	public String getInputFilename() {
		return this.inputFilename;
	}

	// Return the output filename
	public String getOutputFilename() {
		return this.outputFilename;
	}

	// Return the person number
	public int getPersonNum() {
		return this.personNum;
	}

	// Return the time interval in minutes
	public int getTimeInterval() {
		return this.timeInterval;
	}

	public String getDataDir() {
		return dataDir;
	}
}
