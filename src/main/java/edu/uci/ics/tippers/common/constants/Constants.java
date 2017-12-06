package edu.uci.ics.tippers.common.constants;

import java.time.Duration;

/*
Class containing a list of several constants used in the code
 */
public class Constants {

    public static final String GRIDDB_OBS_PREFIX = "TS_Obs_";

    public static final String GRIDDB_SO_PREFIX = "TS_SO_";

    public static final Duration MAX_DURATION = Duration.ofSeconds(10000000, 0);

    public static String CONFIG = "benchmark.ini";

    public static long SHUTDOWN_WAIT = 1000;

    public static String QUERY_FILE_FORMAT = "query%s.txt";

    public static int PGSQL_BATCH_SIZE = 50000;

    public static int MONGO_BATCH_SIZE = 50000;


    public static String ASTERIX_FEED_IP = "localhost";

    public static int ASTERIX_FEED_PORT = 39011;

    public static int LOG_LIM = 100000;


}
