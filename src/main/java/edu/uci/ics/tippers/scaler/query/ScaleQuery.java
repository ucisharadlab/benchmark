package edu.uci.ics.tippers.scaler.query;

// Author: Fangfang Fu
// Date: 7/15/2017

// query type 1 
// Select_Sensor(X) Query
// select * from sensor where id = X


import edu.uci.ics.tippers.scaler.query.parser.*;

import java.io.IOException;
import java.util.*;

// Query includes the main function to call all the query generator
public class ScaleQuery
{

    private QueryConfiguration confi;

    public ScaleQuery(QueryConfiguration confi) {
        this.confi = confi;
    }

    public void generateQueries () throws IOException {

    	QueryGenerator query = new QueryGenerator();

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

