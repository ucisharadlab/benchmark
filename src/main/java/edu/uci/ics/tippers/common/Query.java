package edu.uci.ics.tippers.common;

import java.util.HashMap;
import java.util.Map;

public enum Query {

    INSERT_SINGLE(-1, "Select a sensor with id X"),
    INSERT_COMPLETE(0, "Select a sensor with id X"),
    SELECT_SENSOR(1, "Select a sensor with id X"),
    SPACE_TO_SENSOR(2, "List all sensor of type X that can observe Locations in {locations}"),
    OBSERVATIONS_SINGLE(3, "Select Observations From a Sensor with id X between time range T1 and T2."),
    OBSERATIONS_MULTIPLE(4, "Select Observations From Sensors in list {X1, X2 ….} between time range T1 and T2."),
    OBSERVATIONS_PAYLOAD(5, "Select Observations of Sensors of type X between time range T1 and T2 and payload.Y in range (Y_a, Y_b)"),
    STATISTICS(6, "Average number of observations per day between the begin and end dates for each sensor in {sensors}"),
    TRAJECTORIES(7, "Fetch names of users who went from Location loc1 to Location loc2  on the specified  date"),
    COLOCATE(8, "Select all users who were in the same Location as User X on a specified date"),
    TIME_SPENT(9, "Fetch average time spent per day by User X in Locations of Type Y."),
    OCCUPANCY(10, "occupancy as a function of time between begin-time and end-time ");

    private final int qNum;
    private final String description;

    private static final Map<Integer, Query> lookup = new HashMap<>();

    static {
        for (Query d : Query.values()) {
            lookup.put(d.getQNum(), d);
        }
    }

    private Query(int qNum, String description) {
        this.qNum = qNum;
        this.description = description;
    }

    public int getQNum() {
        return qNum;
    }

    public String getDescription() {
        return description;
    }

    public static Query get(int num) {
        return lookup.get(num);
    }
}
