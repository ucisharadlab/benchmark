package edu.uci.ics.tippers.query;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static edu.uci.ics.tippers.common.util.Helper.getFileFromQuery;

public abstract class BaseQueryManager {

    protected int mapping;
    protected boolean writeOutput;
    protected String queriesDir;
    protected String outputDir;
    protected long timeout;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public BaseQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        this.mapping = mapping;
        this.writeOutput = writeOutput;
        this.outputDir = outputDir;
        this.queriesDir = queriesDir;
        this.timeout = timeout;
    }

    public abstract Database getDatabase();

    public int getMapping() {
        return this.mapping;
    }

    public int setMapping() {
        return this.mapping;
    }

    private Duration runWithThread(Callable<Duration> query) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Duration> future = executorService.submit(query);
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        }catch (TimeoutException e) {
            e.printStackTrace();
            return Constants.MAX_DURATION;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Running Query");
        } finally {
            executorService.shutdown();
            try {
                executorService.awaitTermination(Constants.SHUTDOWN_WAIT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cleanUp();
        }
    }

    private void warmingUp() {
        QueryCSVReader reader = new QueryCSVReader(queriesDir + getFileFromQuery(1));
        String[] values;
        while ((values = reader.readNextLine()) != null) {
            String sensorId = values[1];
            runQuery1(sensorId);
        }
    }

    public Map<Integer, Duration> runQueries() throws BenchmarkException{

        // Warming up the database by running certain queries
        warmingUp();

        Map<Integer, Duration> queryRunTimes = new HashMap<>();

        // Query 1
        QueryCSVReader reader = new QueryCSVReader(queriesDir + getFileFromQuery(1));
        String[] values;
        int numQueries = 0;
        Duration runTime = Duration.ofSeconds(0);

        try {
            while ((values = reader.readNextLine()) != null) {
                String sensorId = values[1];
                runTime = runTime.plus(runWithThread(() -> runQuery1(sensorId)));
                numQueries++;
            }

            queryRunTimes.put(1, runTime.dividedBy(numQueries));
        } catch (Exception e) {
            e.printStackTrace();
            queryRunTimes.put(1, Constants.MAX_DURATION);
        }

        // Query 2
        numQueries = 0;
        runTime = Duration.ZERO;
        reader = new QueryCSVReader(queriesDir + getFileFromQuery(2));
        try {
            while ((values = reader.readNextLine()) != null) {
                String sensorTypeName = values[1];
                List<String> locations = Arrays.asList(values[2].split(";"));
                runTime = runTime.plus(runWithThread(()->runQuery2(sensorTypeName, locations)));
                numQueries++;
            }
            queryRunTimes.put(2, runTime.dividedBy(numQueries));
        } catch (Exception e) {
            e.printStackTrace();
            queryRunTimes.put(2, Constants.MAX_DURATION);
        }

        // Query 3
        numQueries = 0;
        runTime = Duration.ZERO;
        reader = new QueryCSVReader(queriesDir + getFileFromQuery(3));
        try {
            while ((values = reader.readNextLine()) != null) {
                String sensorId = values[1];
                final Date start, end;
                try {
                    start = sdf.parse(values[2]);
                    end = sdf.parse(values[3]);
                    runTime = runTime.plus(runWithThread(() -> runQuery3(sensorId, start, end)));
                    numQueries++;
                } catch (ParseException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Queries, Incorrect Date Format");
                }

            }
            queryRunTimes.put(3, runTime.dividedBy(numQueries));
        } catch (Exception e) {
            e.printStackTrace();
            queryRunTimes.put(3, Constants.MAX_DURATION);
        }

        // Query 4
        numQueries = 0;
        runTime = Duration.ZERO;
        reader = new QueryCSVReader(queriesDir + getFileFromQuery(4));
        try {
            while ((values = reader.readNextLine()) != null) {
                List<String> sensorIds = Arrays.asList(values[1].split(";"));
                Date start, end;
                try {
                    start = sdf.parse(values[2]);
                    end = sdf.parse(values[3]);
                    runTime = runTime.plus(runWithThread(() -> runQuery4(sensorIds, start, end)));
                    numQueries++;
                } catch (ParseException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Queries, Incorrect Date Format");
                }
            }
            queryRunTimes.put(4, runTime.dividedBy(numQueries));
        } catch (Exception e) {
            e.printStackTrace();
            queryRunTimes.put(4, Constants.MAX_DURATION);
        }

        // Query 5
        numQueries = 0;
        runTime = Duration.ZERO;
        reader = new QueryCSVReader(queriesDir + getFileFromQuery(5));
        try {
            while ((values = reader.readNextLine()) != null) {
                String sensorTypeName = values[1];
                Date start, end;
                try {
                    start = sdf.parse(values[2]);
                    end = sdf.parse(values[3]);
                    String payload = values[4];
                    String type = values[5];
                    Object startValue;
                    Object endValue;
                    if ("INT".equals(type)) {
                        startValue = Integer.parseInt(values[6]);
                        endValue = Integer.parseInt(values[7]);

                    } else if ("DOUBLE".equals(type)) {
                        startValue = Double.parseDouble(values[6]);
                        endValue = Double.parseDouble(values[7]);
                    } else {
                        startValue = values[6];
                        endValue = values[7];
                    }

                    runTime = runTime.plus(runWithThread(() -> runQuery5(sensorTypeName, start, end,
                            payload, startValue, endValue)));
                    numQueries++;
                } catch (ParseException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Queries, Incorrect Date Format");
                }
            }
            queryRunTimes.put(5, runTime.dividedBy(numQueries));
        } catch (Exception e) {
            e.printStackTrace();
            queryRunTimes.put(5, Constants.MAX_DURATION);
        }

        // Query 6
        numQueries = 0;
        runTime = Duration.ZERO;
        reader = new QueryCSVReader(queriesDir + getFileFromQuery(6));
        try {
            while ((values = reader.readNextLine()) != null) {
                List<String> sensorIds = Arrays.asList(values[1].split(";"));
                Date start, end;
                try {
                    start = sdf.parse(values[2]);
                    end = sdf.parse(values[3]);
                    runTime = runTime.plus(runWithThread(() -> runQuery6(sensorIds, start, end)));
                    numQueries++;
                } catch (ParseException e) {
                    e.printStackTrace();
                    throw new BenchmarkException("Error Running Queries, Incorrect Date Format");
                }
            }
            queryRunTimes.put(6, runTime.dividedBy(numQueries));
        } catch (Exception e) {
            e.printStackTrace();
            queryRunTimes.put(6, Constants.MAX_DURATION);
        }
        return queryRunTimes;
    }

    public abstract void cleanUp();

    public abstract Duration runQuery1(String sensorId) throws BenchmarkException;

    public abstract Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException;

    public abstract Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException;

    public abstract Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException;

    public abstract Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                                   Object startPayloadValue, Object endPayloadValue) throws BenchmarkException;

    public abstract Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException;
}
