package edu.uci.ics.tippers.query;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public abstract class BaseQueryManager {

    protected int mapping;
    protected boolean writeOutput;
    protected String queriesDir;
    protected long timeout;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public BaseQueryManager(int mapping, String queriesDir, boolean writeOutput, long timeout) {
        this.mapping = mapping;
        this.writeOutput = writeOutput;
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
        QueryCSVReader reader = new QueryCSVReader(queriesDir + "query1.txt");
        String[] values;
        while ((values = reader.readNextLine()) != null) {
            String sensorId = values[1];
            runQuery1(sensorId);
        }
    }

    public Map<Integer, Duration> runQueries() throws BenchmarkException{

        warmingUp();

        Map<Integer, Duration> queryRunTimes = new HashMap<>();

        QueryCSVReader reader = new QueryCSVReader(queriesDir + "query1.txt");
        String[] values;
        int numQueries = 0;
        Duration runTime = Duration.ofSeconds(0);

        while ((values = reader.readNextLine()) != null) {
            String sensorId = values[1];
            runTime = runTime.plus(runWithThread(()->runQuery1(sensorId)));
            numQueries++;
        }

        queryRunTimes.put(1, runTime.dividedBy(numQueries));

        numQueries = 0;
        runTime = Duration.ZERO;
        reader = new QueryCSVReader(queriesDir + "query2.txt");
        while ((values = reader.readNextLine()) != null) {
            String sensorTypeName = values[1];
            List<String> locations = Arrays.asList(values[2].split(";"));
            runTime = runTime.plus(runWithThread(()->runQuery2(sensorTypeName, locations)));
            numQueries++;
        }
        queryRunTimes.put(2, runTime.dividedBy(numQueries));

        numQueries = 0;
        runTime = Duration.ZERO;
        reader = new QueryCSVReader(queriesDir + "query3.txt");
        while ((values = reader.readNextLine()) != null) {
            String sensorId = values[1];
            final Date start, end;
            try {
                start = sdf.parse(values[2]);
                end = sdf.parse(values[3]);
                runTime = runTime.plus(runWithThread(()->runQuery3(sensorId, start, end)));
                numQueries++;
            } catch (ParseException e) {
                e.printStackTrace();
                throw new BenchmarkException("Error Running Queries, Incorrect Date Format");
            }

        }
        queryRunTimes.put(3, runTime.dividedBy(numQueries));

        numQueries = 0;
        runTime = Duration.ZERO;
        reader = new QueryCSVReader(queriesDir + "query4.txt");
        while ((values = reader.readNextLine()) != null) {
            List<String> sensorIds = Arrays.asList(values[1].split(";"));
            Date start, end;
            try {
                start = sdf.parse(values[2]);
                end = sdf.parse(values[3]);
                runTime = runTime.plus(runWithThread(()->runQuery4(sensorIds, start, end)));
                numQueries++;
            } catch (ParseException e) {
                e.printStackTrace();
                throw new BenchmarkException("Error Running Queries, Incorrect Date Format");
            }
        }
        queryRunTimes.put(4, runTime.dividedBy(numQueries));

        return queryRunTimes;
    }

    public abstract void cleanUp();

    public abstract Duration runQuery1(String sensorId) throws BenchmarkException;

    public abstract Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException;

    public abstract Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException;

    public abstract Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException;

    public abstract Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                                   Object startPayloadValue, Object endPayloadValue) throws BenchmarkException;

}
