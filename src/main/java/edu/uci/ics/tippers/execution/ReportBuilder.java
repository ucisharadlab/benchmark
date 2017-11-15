package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.ReportFormat;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.exception.BenchmarkException;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

/* This class is used to build a report in different formats after
the benchmark is executed successfully
 */
public class ReportBuilder {

    private ReportFormat format;
    private Map<Pair<Database, Integer>, Map<Integer, Duration>> runTimes;
    private String reportsDir;
    private DataSize dataSize;

    public ReportBuilder(Map<Pair<Database, Integer>, Map<Integer, Duration>> runTimes, String reportsDir,
                         ReportFormat format, DataSize dataSize){
        this.format = format;
        this.runTimes = runTimes;
        this.reportsDir = reportsDir;
        this.dataSize = dataSize;
    }

    private void createPDFReport() {
        // TODO: Yet To Implement
    }

    private void createCSVReport() {
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(reportsDir + "report.csv"));
            writer.write("Database, Mapping, Insert 90%, Insert 10%, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10\n");

            for(Pair<Database, Integer> key: runTimes.keySet()) {

                String line = String.format("%s,%s",key.getKey().getName(), key.getValue());
                Map<Integer, Duration> queryTimes = runTimes.get(key);

                if (queryTimes == null) {
                    line += String.format(",%s","None\n");
                    writer.write(line);
                    continue;
                }
                for (Integer query: queryTimes.keySet()) {
                    if (queryTimes.get(query).compareTo(Constants.MAX_DURATION) < 0 )
                        line += String.format(",%s", queryTimes.get(query).toMillis());
                    else
                        line += String.format(",%s",  "KIA" );
                }
                line += "\n";
                writer.write(line);
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Writing Report.csv");
        }
    }

    private void createTextReport() throws BenchmarkException {
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(reportsDir + "report.txt"));

            //dataSize.appendInfoToFile(writer);

            writer.write("------------- Insertion And Query Times----------\n\n");

            writer.write(String.format("%s%10s%25s\n", "Database", "Mapping", "Query;RunTime(ms)"));
            writer.write("------------------------------------------------------------------------------------\n");

            for(Pair<Database, Integer> key: runTimes.keySet()) {

                String line = String.format("%s%5s",key.getKey().getName(), key.getValue());
                Map<Integer, Duration> queryTimes = runTimes.get(key);

                if (queryTimes == null) {
                    line += String.format("%10s","None\n");
                    writer.write(line);
                    writer.write("------------------------------------------------------------------------------------\n");
                    continue;
                }
                for (Integer query: queryTimes.keySet()) {
                    if (queryTimes.get(query).compareTo(Constants.MAX_DURATION) < 0 )
                        line += String.format("%10s", queryTimes.get(query).toMillis());
                    else
                        line += String.format("%10s",  "KIA" );
                }
                line += "\n";
                writer.write(line);
                writer.write("------------------------------------------------------------------------------------\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Writing Report.txt");
        }
    }

    private void createHTMLReport() {
        // TODO: Yet To Implement
    }

    public void createReport() throws BenchmarkException {
        switch (format) {
            case PDF:
                createPDFReport();
                break;
            case HTML:
                createHTMLReport();
                break;
            case TEXT:
                createTextReport();
                break;
            case CSV:
                createCSVReport();
                break;
        }
    }

}
