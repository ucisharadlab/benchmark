import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.time.LocalDate;

// Query generator class
public class QueryGenerator 
{
	private QueryTemplate template;
	
	// constructor
	public QueryGenerator() {
		template = new QueryTemplate();
	}
	
	
	// Select_Sensor(X):  Select a sensor with id X
    public void SelectSensorGenerator(List<String> sensorIds, int seed, int queryNum) {
        Random rand = new Random(seed);               // use constant seed
        List<Integer>indexList = new ArrayList<Integer>(); // record generated index
        
        // generate unique query
        for (int j = 0; j < queryNum; ++j) {
            int size = sensorIds.size();
            int n = rand.nextInt(size);

            // check duplicate
            if (!indexList.contains(n)) {
                template.Select_Sensor(sensorIds.get(n));
                indexList.add(n);
            } else {
                j -= 1;
            } 
        }
    }


    // Space_to_Sensor(X, {locations}): List all sensor of type X that can observe Locations in {locations}.
    public void SpaceToSensorGenerator(List<String> sensorTypeIds, List<String> locationIds, 
        int seed, int queryNum) {
        Random rand = new Random(seed);                    // use constant seed
        List<Integer> indexList = new ArrayList<Integer>();

        // generate random number of unique locations
        int locationSize = locationIds.size();
        int locationNum = rand.nextInt(locationSize) + 1;
        
        for (int i = 0; i < queryNum; ++i) {
            int sensorTypeSize = sensorTypeIds.size();
            int n = rand.nextInt(sensorTypeSize);
            List<String> locations = generateListOfLocations(locationIds, n+1, locationNum);

            // check duplicate
            if (!indexList.contains(n)) {
                template.Space_to_Sensor(sensorTypeIds.get(n), locations);
                indexList.add(n);
            } else {
                i -= 1;
            } 
        }
    }

    
    // Observations(X, <T1, T2>): Select Observations From a Sensor with id X between time range T1 and T2.
    public void ObservationOfSingleSensorGenerator(List<String> sensorIds, List<String> timestamps, 
        int seed, int queryNum) {
        Random rand = new Random(seed);   // use consant seed

        // generate random unique ids and then print queries
        int sensorSize = sensorIds.size();
        for (int i = 0; i < queryNum; ++i) {
            int n = rand.nextInt(sensorSize);
            List<String> pairTimes = generatePairTimes(timestamps, n+1);
            template.Observations1(sensorIds.get(n), pairTimes);
            Collections.swap(sensorIds, n, sensorSize-1);
            sensorSize -= 1;
        }
    }
    
    
    // Observations({X1,X2, ...}, <T1, T2>): Select Observations From Sensors in list {X1, X2 ...} between time range T1 and T2.
    public void ObservationOfMultipleSensorGenerator(List<String>sensorIds, List<String>timestamps, 
        int seed, int queryNum) {
        Random rand = new Random(seed); 
        // generate a random unique list of sensor ids and then print queries
        int sensorSize = sensorIds.size();
        for (int i = 0; i < queryNum; ++i) {
            int n = rand.nextInt(sensorSize) + 1; // relative random seed, would not affect constant data
            List<String> randSensorIds = generateListOfSensorIds(sensorIds, n);
            List<String> pairTimes = generatePairTimes(timestamps, n);
            template.Observations2(randSensorIds, pairTimes);
        }
    }
    
    
    /* Observations(X,<T1,T2>, Y, <Y_a, Y_b>): Select Observations of Sensors of type X between time range T1 and T2 and
    payload.Y in range (Y_a, Y_b) */
	public void ObservationOfSensorTypeGenerator(List<String> sensorTypeIds, List<String> timestamps, 
	 		List<Integer> payload, int seed, int queryNum) {
	 	Random rand = new Random(seed);               // use consant seed
	 	int sensorTypeSize = sensorTypeIds.size();
	 	
	 	for (int i = 0; i < queryNum; ++i) {
	 		int n = rand.nextInt(sensorTypeSize);
	 		List<String> pairTimes = generatePairTimes(timestamps, n+1); // 
	 		List<Integer> pairPayload = generatePairPayload(payload, n+1);
	 		template.Observations3(sensorTypeIds.get(n), pairTimes, pairPayload);
	 		Collections.swap(sensorTypeIds, n, sensorTypeSize-1);
	 		sensorTypeSize -= 1;
	 	}
	}
	
	
	/* Statistics({sensors}, <begin-date, end-date> ): Average number of observations per day between the begin 
     * and end dates for each sensor in {sensors} 
     */
	public void StatisticsGenerator(List<String> sensorIds, int seed, int queryNum) {
		for (int i = 0; i < queryNum; ++i) {
			List<LocalDate> pairDates = generateDates(seed+i, 2);
			
			// make sure second date is bigger than first one
			if (pairDates.get(0).isAfter(pairDates.get(1))) {
	        	Collections.swap(pairDates, 0, 1);
	        }
			
			template.Statistics(sensorIds, pairDates);
		}
	}
	
	
	// Trajectories(date, loc1, loc2): Fetch names of users who went from Location loc1 to Location loc2  on the specified  date.
	public void TrajectoriesGenerator(List<String> locationIds, int seed, int queryNum) {    
		for (int i = 0; i < queryNum; ++i) {
            LocalDate date = generateDates(seed+i, 1).get(0);
            List<String> pairLocations = generateListOfLocations(locationIds, seed+i, 2);
            template.Trajectories(date, pairLocations);
		}
	}
	
	
	// Colocate(X, date): Select all users who were in the same Location as User X on a specified date.
	public void ColocateGenerator(List<String> userIds, int seed, int queryNum) {
		Random rand = new Random(seed);
		int userSize = userIds.size();

		for (int i = 0; i < queryNum; ++i) {
			LocalDate date = generateDates(seed+i, 1).get(0);
			int n = rand.nextInt(userSize);
			template.Colocate(userIds.get(n), date);
			Collections.swap(userIds, n, userSize-1);
			--userSize;
		}
	}
	
	
	// Time_Spent(X, Y) Fetch average time spent per day by User X in Locations of Type Y. 
	public void TimeSpentGenerator(List<String> userIds, List<String> locationTypes, int seed, int queryNum) {
		Random rand = new Random(seed);
		int userSize = userIds.size();
		int locationTypeSize = locationTypes.size();
		
		for (int i = 0; i < queryNum; ++i) {
			int n = rand.nextInt(userSize);
			int m = rand.nextInt(locationTypeSize);
			template.Time_Spent(userIds.get(n), locationTypes.get(m));
			Collections.swap(userIds, n, userSize-1);
			--userSize;	
		}
	}
	
	
	// Occupancy({locations}, time-unit, <begin-time, end-time>): occupancy as a function of time between begin-time and end-time
	public void OccupancyGenerator(List<String>locationIds, List<String>timestamps, int seed, int queryNum) {
		Random rand = new Random(seed);
		int count = 1;
		List<String> timeUnits = new ArrayList<>(Arrays.asList("minute", "hour", "day", "month", "year"));
		int timeUnitSize = timeUnits.size();
		
		for (int i = 0; i < queryNum; ++i) {
			// generate time unit
			if (count > 10) {
				count = 1;
			}
			int n = rand.nextInt(timeUnitSize);
			
			List<String> pairTimestamps = generatePairTimes(timestamps, seed+i);
			template.Occupancy(locationIds, timeUnits.get(n), pairTimestamps);
		}
	}
	
	
	// Generate a pair of unique random dates 
	public List<LocalDate> generateDates(int seed, int num) {
		Random rand = new Random(seed);
        
		List<LocalDate> dates = new ArrayList<LocalDate>(num);
		
        long startDate = LocalDate.of(2017, 1, 1).toEpochDay(); //start date
        long endDate = LocalDate.now().toEpochDay(); //end date 
        int dateSize = (int)(endDate - startDate + 1);
        
        for (int i = 0; i < num; ++i) {
        	long randomEpochDay = rand.nextInt(dateSize) + startDate;
            LocalDate randDate= LocalDate.ofEpochDay(randomEpochDay);
            if (!dates.contains(randDate)) {
            	dates.add(randDate);
            } else {
            	--i;
            } 
        }
        
        return dates;
	}
	
	
	// Generate a list of unique sensor Ids 
    public List<String> generateListOfSensorIds(List<String> sensorIds, int seed) {
        Random rand = new Random(seed);          // use consant seed
        
        int sensorSize = sensorIds.size();
        int sensorNum =rand.nextInt(sensorSize) + 1;
        List<String> listOfSensorIds = new ArrayList<String>(sensorNum);

        for (int i = 0; i < sensorNum; ++i) {
            int n = rand.nextInt(sensorSize);
            listOfSensorIds.add(sensorIds.get(n));
            Collections.swap(sensorIds, n, sensorSize-1);
            sensorSize -= 1;
        }

        return listOfSensorIds;
    }

    
    // Generate a list of unique locations
    public List<String> generateListOfLocations(List<String> locationIds, int seed, int locationNum) {
        Random rand = new Random(seed);                    // use consant seed

        // generate random number of unique locations
        int locationSize = locationIds.size();
        List<String> locations = new ArrayList<String>(locationNum);

        for (int i = 0; i < locationNum; ++i) {
            int n = rand.nextInt(locationSize);
            locations.add(locationIds.get(n));
            Collections.swap(locationIds, n, locationSize-1);
            locationSize -= 1;
        }

        return locations;
    }
    
    
    // Generate a pair of unique times 
    public List<String> generatePairTimes(List<String> timestamps, int seed) {
        Random rand = new Random(seed);                    // use consant seed

        // generate a unique pair of times
        int timeSize = timestamps.size();
        List<String> pairTimes = new ArrayList<String>(2);

        int n = rand.nextInt(timeSize);
        pairTimes.add(timestamps.get(n));
        Collections.swap(timestamps, n, timeSize-1);
        timeSize -= 1;

        n = rand.nextInt(timeSize);
        pairTimes.add(timestamps.get(n));

        // convert string to Timestamp
        Timestamp time1 = Timestamp.valueOf(pairTimes.get(0));
        Timestamp time2 = Timestamp.valueOf(pairTimes.get(1));
        
        if (time1.after(time2)) {
            Collections.swap(pairTimes, 0, 1);
        }

        return pairTimes;
    }
    
    
    // generate a pair of unique payloads
    public List<Integer> generatePairPayload(List<Integer> payload, int seed) {
    	Random rand = new Random(seed);
    	
    	// generate a unique pair of payload: second one is larger than first one
    	int payloadSize = payload.size();
    	List<Integer> pairPayload = new ArrayList<Integer>(2);

        int n = rand.nextInt(payloadSize);
        pairPayload.add(payload.get(n));
        Collections.swap(payload, n, payloadSize-1);
        payloadSize -= 1;

        n = rand.nextInt(payloadSize);
        pairPayload.add(payload.get(n));

        // convert string to integers
        
        if (pairPayload.get(0) > pairPayload.get(1)) {
            Collections.swap(pairPayload, 0, 1);
        }

        return pairPayload;
    } 
}
