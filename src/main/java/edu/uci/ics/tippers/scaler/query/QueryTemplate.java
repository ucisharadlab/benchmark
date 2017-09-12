package edu.uci.ics.tippers.scaler.query;

import java.time.LocalDate;
import java.util.List;

// This class includes all templates
public class QueryTemplate 
{
	// Select_Sensor(X):  Select a sensor with id X
    public void Select_Sensor(String X) {
        System.out.println("select * from sensor where id = " + X + ";");
    }

    
    // Space_to_Sensor(X, {locations}): List all sensor of type X that can observe Locations in {locations}. 
    public void Space_to_Sensor(String X, List<String> locations) {
        System.out.print("select * from sensor where typeId = " + X 
            + " and (infraStructureId in (");

        int locationSize = locations.size();
        for(int i = 0; i < locationSize-1; ++i) {
            System.out.print(locations.get(i) + ", ");
        }

        System.out.println(locations.get(locationSize-1) + "));");           
    }
  
    
    // Observations(X, <T1, T2>): Select Observations From a Sensor with id X between time range T1 and T2.
    public void Observations1(String X, List<String> times) {
        System.out.println("select * from observation where sensorId = " + X
            + " and (timestamp between " + times.get(0) + " and " + times.get(1) + ");");
    }
     
    
    // Observations({X1,X2, ...}, <T1, T2>): Select Observations From Sensors in list {X1, X2 ...} between time range T1 and T2.
    public void Observations2(List<String> sensorIds, List<String> times) {
        System.out.print("select * from observation where (sensorId in ");
        int size = sensorIds.size();
        for (int i = 0; i < size-1; ++i) {
            System.out.print(sensorIds.get(i) + ", ");
        }
        System.out.println(sensorIds.get(size-1) + ") and (timestamp between "
            + " " + times.get(0) + " and " + times.get(1) + ");");
    }
    
    
    /* Observations(X,<T1,T2>, Y, <Y_a, Y_b>): Select Observations of Sensors of type X between time range T1 and T2 
       and payload.Y in range (Y_a, Y_b) */
    public void Observations3(String typeId, List<String> pairTimes, List<Integer> pairPayload) {
    	System.out.println("select * from observation where typeId = " + typeId + "Type and (timestamp between "
    			+ pairTimes.get(0) + " and " + pairTimes.get(1) + ") and (payload.Y between " + pairPayload.get(0) 
    			+ " and " + pairPayload.get(1) + ");");
    }
    
    
    /* Statistics({sensors}, <begin-date, end-date> ): Average number of observations per day between the begin 
     * and end dates for each sensor in {sensors} 
     */
    public void Statistics(List<String> sensorIds, List<LocalDate> pairDate) {
    	System.out.println("SELECT COUNT(id) FROM observation WHERE timestamp BETWEEN " + pairDate.get(0) + " AND " 
    			+ pairDate.get(1) + " GROUP BY sensorId;");
    } 
    
    
    // Trajectories(date, loc1, loc2): Fetch names of users who went from Location loc1 to Location loc2  on the specified  date.
    public void Trajectories(LocalDate date, List<String> pairLocations) {
    	String loc1 = pairLocations.get(0);
    	String loc2 = pairLocations.get(1);
    	System.out.println("SELECT name FROM user WHERE id in (SELECT semanticEntityId from (SELECT semanticEntityId , timestamp as startTime "
    			+ "FROM semanticObservation WHERE payload.location = " + loc1 + "and (timestamp BETWEEN " + date + " 00:00:00 AND " + date + " 23:59:59)"
    			+ "and semanticEntityId in (SELECT semanticEntityId FROM semanticObservation WHERE payload.location = " + loc2 
    			+ "and (timestamp BETWEEN startTime and " + date + "23:59:59))));");
    }
    
    // Colocate(X, date): Select all users who were in the same Location as User X on a specified date.
    public void Colocate(String userId, LocalDate date) {
    	System.out.println("select * from user where id in (select semanticEntityId from semanticObservation "
    			+ "where payload.location = (select payload.location "
    			+ "from semanticObservation where semanticEntityId = " + userId + " and timestamp = " + date + "));");
    }
    
    // Time_Spent(X, Y) Fetch average time spent per day by User X in Locations of Type Y. 
    public void Time_Spent(String userId, String locationType) {
    	System.out.println("Query9: " + userId + ", " + locationType);
    }
    
    // Occupancy({locations}, time-unit, <begin-time, end-time>): occupancy as a function of time between begin-time and end-time
    public void Occupancy(List<String> locations, String timeUnit, List<String> pairTimestamps) {
    	System.out.println("Query10: " + locations.get(0) + ", " + timeUnit + ", " +  pairTimestamps.get(0) + ", " + pairTimestamps.get(1));
    }
}
