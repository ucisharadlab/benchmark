package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.ReportFormat;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.exception.BenchmarkException;
import javafx.util.Pair;

import javax.xml.crypto.Data;
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

    public ReportBuilder(Map<Pair<Database, Integer>, Map<Integer, Duration>> runTimes, String reportsDir,
                         ReportFormat format){
        this.format = format;
        this.runTimes = runTimes;
        this.reportsDir = reportsDir;
    }

    private void createPDFReport() {
        // TODO: Yet To Implement
    }

    private void createTextReport() throws BenchmarkException {
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(reportsDir + "report.txt"));
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
                    if (queryTimes.get(query) != Constants.MAX_DURATION)
                        line += String.format("%10s", query+";"+queryTimes.get(query).toMillis());
                    else
                        line += String.format("%10s", query + ";" + "None" );
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
        }
    }

}
