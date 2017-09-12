package edu.uci.ics.tippers.scaler.query;

// Author: Fangfang Fu
// Date: 7/15/2017

// query type 1 
// Select_Sensor(X) Query
// select * from sensor where id = X


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

// Query includes the main function to call all the query generator
public class Query
{
    public static void main (String[] args) throws IOException {
    	QueryGenerator query = new QueryGenerator();
    	// parse configure file
    	Configure confi = new Configure();
    	confi.parseFile();
    	
        // parse sensor file
        Sensor sensors = new Sensor();
        sensors.parseData();
        List<String> sensorIds = sensors.getIds();
        // List<String> sensorTypeIds = sensors.getTypeIds();
        List<String> locationIds = sensors.getInfraStructureIds();
        
        // parse sensorType file
        SensorType sensorTypes = new SensorType();
        sensorTypes.parseData();
        List<String> sensorTypeIds = sensorTypes.getIds();
   
        // parse observation file
        Observation observations = new Observation();
        observations.parseData();
        // List<String> timestamps = observations.getTimestamps();
        
        // parse temperatureObs file
        TemperatureObs temperatureObs = new TemperatureObs();
        temperatureObs.parseData();
        // List<String> typeIds = temperatureObs.getTypeIds();
        List<String> timestamps = temperatureObs.getTimestamps();
        List<Integer> payloads = temperatureObs.getPayloads();
        
        // parse user file
        User users = new User();
        users.parseData();
        List<String> userIds = users.getIds();
        
        // parse infrastructureType file
        InfrastructureType infrastructureTypes = new InfrastructureType();
        infrastructureTypes.parseData();
        List<String> locationTypes = infrastructureTypes.getIds();
        
        // Select_Sensor(X):  Select a sensor with id X
        int selectSensorNum = confi.getSelectSensorNum(); // < 82 sensorId number
        int selectSensorseed = confi.getSelectSensorSeed();
        query.SelectSensorGenerator(sensorIds, selectSensorseed, selectSensorNum);

        // Space_to_Sensor(X, {locations}): List all sensor of type X that can observe Locations in {locations}.
        System.out.println();
        int spaceSensorNum = confi.getSpaceSensorNum(); // <= 6 sensor Types
        int spaceSensorSeed = confi.getSpaceSensorSeed();
        query.SpaceToSensorGenerator(sensorTypeIds, locationIds, spaceSensorSeed, spaceSensorNum);

        // Observations(X, <T1, T2>): Select Observations From a Sensor with id X between time range T1 and T2.
        System.out.println();  // <= 82 sensorIds number
        int observeNum1 = confi.getObserveNum1();
        int observeSeed1 = confi.getObserveSeed1();
        query.ObservationOfSingleSensorGenerator(sensorIds, timestamps, observeSeed1, observeNum1);

        // Observations({X1,X2, ...}, <T1, T2>): Select Observations From Sensors in list {X1, X2 ...} between time range T1 and T2.
        System.out.println(); 
        int observeNum2 = confi.getObserveNum2();   // <= 2^82 (did not check duplicate because it has a very small possibility to have duplicate
        int observeSeed2 = confi.getObserveSeed2();
        query.ObservationOfMultipleSensorGenerator(sensorIds, timestamps, observeSeed2, observeNum2);

        /* Observations(X,<T1,T2>, Y, <Y_a, Y_b>): Select Observations of Sensors of type X between time range T1 and T2 
           and payload.Y in range (Y_a, Y_b) */
        System.out.println(); 
        int observeNum3 = confi.getObserveNum3();   // <= 6 sensor Types
        int observeSeed3 = confi.getObserveSeed3();
        query.ObservationOfSensorTypeGenerator(sensorTypeIds, timestamps, payloads, observeSeed3, observeNum3);
        
        /* Statistics({sensors}, <begin-date, end-date> ): Average number of observations per day between the begin 
         * and end dates for each sensor in {sensors} 
         */
        System.out.println(); 
        int statisticsNum = confi.getStatisticsNum(); 
        int statisticSeed = confi.getStatisticsSeed();
        query.StatisticsGenerator(sensorIds, statisticSeed, statisticsNum);
        
        // Trajectories(date, loc1, loc2): Fetch names of users who went from Location loc1 to Location loc2  on the specified  date.
        System.out.println(); 
        int trajectoriesNum = confi.getTrajectoriesNum();
        int trajectoriesSeed = confi.getTrajectoriesSeed();
        query.TrajectoriesGenerator(locationIds, trajectoriesSeed, trajectoriesNum);
        
        // Colocate(X, date): Select all users who were in the same Location as User X on a specified date.
        System.out.println(); 
        int colocateNum = confi.getColocateNum();  // < 14 userId number
        int colocateSeed = confi.getColocateSeed();
        query.ColocateGenerator(userIds, colocateSeed, colocateNum);
        
        // Time_Spent(X, Y): Fetch average time spent per day by User X in Locations of Type Y.
        System.out.println(); 
        int timeSpentNum = confi.getTimeSpentNum(); // < 14 userId number
        int timeSpentSeed = confi.getTimeSpentSeed();
        query.TimeSpentGenerator(userIds, locationTypes, timeSpentSeed, timeSpentNum);
        
        // Occupancy({locations}, time-unit, <begin-time, end-time>): occupancy as a function of time between begin-time and end-time
        System.out.println(); 
        int occupancyNum = confi.getOccupancyNum(); 
        int occupancySeed = confi.getOccupancySeed();
        query.OccupancyGenerator(locationIds, timestamps, occupancySeed, occupancyNum);
    }
    
}

//Read configure.txt file
class Configure {
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
	public Configure() {
		this.selectSensorNum = 1;
		this.selectSensorSeed = 1;
		this.spaceSensorNum = 1;
		this.spaceSensorSeed = 1;
		this.observeNum1 = 1;
		this.observeSeed1 = 1;
		this.observeNum2 = 1;
		this.observeSeed2 = 1;
		this.observeNum3 = 1;
		this.observeSeed3 = 1;
		this.statisticsNum = 1;
		this.statisticsSeed = 1;
		this.trajectoriesNum = 1;
		this.trajectoriesSeed = 1;
		this.colocateNum = 1;
		this.colocateSeed = 1;
		this.timeSpentNum = 1;
		this.timeSpentSeed = 1;
		this.occupancyNum = 1;
		this.occupancySeed = 1;
	}
	
	// Read lines number
	private int countNumOfLines() throws IOException {
		FileReader fileReader = new FileReader("configure.txt");
		BufferedReader bf = new BufferedReader(fileReader);
		
		int numberOfLines = 0;
		
		while (bf.readLine() != null) {
			numberOfLines++;
		}	
		bf.close();
		
		return numberOfLines;		
	}
	
	// Read configure.txt file and extract scale number
	public void parseFile() throws IOException {
		// read file to array
		int numberOfLines = countNumOfLines();
		FileReader fileReader = new FileReader("configure.txt");
		BufferedReader bf = new BufferedReader(fileReader);
		String[] textData = new String[numberOfLines];
		
		for (int i = 0; i < numberOfLines; ++i) {
			textData[i] = bf.readLine();
			String[] result = textData[i].split(":\\s");
			
			if (result[0].equals("selectSensorNum")) {  // extract sensorType
				this.selectSensorNum = Integer.parseInt(result[1]);
			} else if (result[0].equals("selectSensorSeed")) {   // extract time scale number
				this.selectSensorSeed = Integer.parseInt(result[1]);
			} else if (result[0].equals("spaceSensorNum")) {  // extract speed scale number
				this.spaceSensorNum = Integer.parseInt(result[1]);
			} else if (result[0].equals("spaceSensorSeed")) { // extract device scale number
				this.spaceSensorSeed = Integer.parseInt(result[1]);
			} else if (result[0].equals("observeNum1")) { // extract time scale noise percent
				this.observeNum1 = Integer.parseInt(result[1]);
			} else if (result[0].equals("observeSeed1")) { // extract speed scale noise percent
				this.observeSeed1= Integer.parseInt(result[1]);
			} else if (result[0].equals("observeNum2")) { // extract device scale noise percent
				this.observeNum2 = Integer.parseInt(result[1]);
			} else if (result[0].equals("observeSeed2")) {   // extract input filename
				this.observeSeed2 = Integer.parseInt(result[1]);
			} else if (result[0].equals("observeNum3")) {  // extract output filename
				this.observeNum3 = Integer.parseInt(result[1]);
			} else if (result[0].equals("observeSeed3")) {  // extract output filename
				this.observeSeed3 = Integer.parseInt(result[1]);
			} else if (result[0].equals("statisticsNum")) {  // extract output filename
				this.statisticsNum= Integer.parseInt(result[1]);
			} else if (result[0].equals("statisticsSeed")) {  // extract output filename
				this.statisticsSeed = Integer.parseInt(result[1]);
			} else if (result[0].equals("trajectoriesNum")) {  // extract output filename
				this.trajectoriesNum = Integer.parseInt(result[1]);
			} else if (result[0].equals("trajectoriesSeed")) {  // extract output filename
				this.trajectoriesSeed = Integer.parseInt(result[1]);
			} else if (result[0].equals("colocateNum")) {  // extract output filename
				this.colocateNum = Integer.parseInt(result[1]);
			} else if (result[0].equals("colocateSeed")) {  // extract output filename
				this.colocateSeed = Integer.parseInt(result[1]);
			} else if (result[0].equals("timeSpentNum")) {  // extract output filename
				this.timeSpentNum = Integer.parseInt(result[1]);
			} else if (result[0].equals("timeSpentSeed")) {  // extract output filename
				this.timeSpentSeed = Integer.parseInt(result[1]);
			} else if (result[0].equals("occupancyNum")) {  // extract output filename
				this.occupancyNum = Integer.parseInt(result[1]);
			} else if (result[0].equals("occupancySeed")) {  // extract output filename
				this.occupancySeed = Integer.parseInt(result[1]);
			}
		}
		
		bf.close();	
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
