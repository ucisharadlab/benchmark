package edu.uci.ics.tippers.scaler.query;

import edu.uci.ics.tippers.common.constants.Constants;
import org.ini4j.Ini;
import org.ini4j.IniPreferences;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.prefs.Preferences;

//Read configure.txt file
public class QueryConfiguration {
	private int selectSensorNum;
	private int selectSensorSeed;
	private int spaceSensorNum;
	private int spaceSensorSeed;
	private int observeNum1;
	private int observeSeed1;
	private int observeNum2;
	private int observeSeed2;
	private int observeNum3;
	private int observeSeed3;
	private int statisticsNum;
	private int statisticsSeed;
	private int trajectoriesNum;
	private int trajectoriesSeed;
	private int colocateNum;
	private int colocateSeed;
	private int timeSpentNum;
	private int timeSpentSeed;
	private int occupancyNum;
	private int occupancySeed;

	// Constructor
	public QueryConfiguration() throws IOException {
		parseFile();
	}

	// Read configure.txt file and extract scale number
	public void parseFile() throws IOException {

		Ini ini = new Ini(new File(getClass().getClassLoader().getResource(Constants.CONFIG).getFile()));
		Preferences prefs = new IniPreferences(ini);

		selectSensorNum = prefs.node("scaledata").getInt("selectSensorNum", 1);
		selectSensorSeed = prefs.node("scaledata").getInt("selectSensorSeed", 1);

		spaceSensorNum = prefs.node("scaledata").getInt("spaceSensorNum", 1);
		spaceSensorSeed = prefs.node("scaledata").getInt("spaceSensorSeed", 1);

		observeNum1 = prefs.node("scaledata").getInt("observeNum1", 1);
		observeSeed1 = prefs.node("scaledata").getInt("observeSeed1", 1);

		observeNum2 = prefs.node("scaledata").getInt("observeNum2", 1);
		observeSeed2 = prefs.node("scaledata").getInt("observeSeed2", 1);

		observeNum3 = prefs.node("scaledata").getInt("observeNum3", 1);
		observeSeed3 = prefs.node("scaledata").getInt("observeSeed3", 1);

		statisticsNum = prefs.node("scaledata").getInt("statisticsNum", 1);
		statisticsSeed = prefs.node("scaledata").getInt("statisticsSeed", 1);

		trajectoriesNum = prefs.node("scaledata").getInt("trajectoriesNum", 1);
		trajectoriesSeed = prefs.node("scaledata").getInt("trajectoriesSeed", 1);

		colocateNum = prefs.node("scaledata").getInt("colocateNum", 1);
		colocateSeed = prefs.node("scaledata").getInt("colocateSeed", 1);

		timeSpentNum = prefs.node("scaledata").getInt("timeSpentNum", 1);
		timeSpentSeed = prefs.node("scaledata").getInt("timeSpentSeed", 1);

		occupancyNum = prefs.node("scaledata").getInt("occupancyNum", 1);
		occupancySeed = prefs.node("scaledata").getInt("occupancySeed", 1);

	}

	// Return select sensor query number
	public int getSelectSensorNum() {
		return this.selectSensorNum;
	}

	public int getSelectSensorSeed() {
		return this.selectSensorSeed;
	}

	public int getSpaceSensorNum() {
		return this.spaceSensorNum;
	}

	public int getSpaceSensorSeed() {
		return this.spaceSensorSeed;
	}

	public int getObserveNum1() {
		return this.observeNum1;
	}

	public int getObserveSeed1() {
		return this.observeSeed1;
	}

	public int getObserveNum2() {
		return this.observeNum2;
	}

	public int getObserveSeed2() {
		return this.observeSeed2;
	}

	public int getObserveNum3() {
		return this.observeNum3;
	}

	public int getObserveSeed3() {
		return this.observeSeed3;
	}

	public int getStatisticsNum() {
		return this.statisticsNum;
	}

	public int getStatisticsSeed() {
		return this.statisticsSeed;
	}

	public int getTrajectoriesNum() {
		return this.trajectoriesNum;
	}

	public int getTrajectoriesSeed() {
		return this.trajectoriesSeed;
	}

	public int getColocateNum() {
		return this.colocateNum;
	}

	public int getColocateSeed() {
		return this.colocateSeed;
	}

	public int getTimeSpentNum() {
		return this.timeSpentNum;
	}

	public int getTimeSpentSeed() {
		return this.timeSpentSeed;
	}

	public int getOccupancyNum() {
		return this.occupancyNum;
	}

	public int getOccupancySeed() {
		return this.occupancySeed;
	}
}
