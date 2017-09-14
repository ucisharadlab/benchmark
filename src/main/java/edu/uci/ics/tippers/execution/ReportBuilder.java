package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.ReportFormat;
import javafx.util.Pair;

import java.time.Duration;
import java.util.Map;

/* This class is used to build a report in different formats after
the benchmark is executed successfully
 */
public class ReportBuilder {

    private ReportFormat format;
    private Map<Pair<Database, Integer>, Map<Integer, Duration>> runTimes;

    public ReportBuilder(Map<Pair<Database, Integer>, Map<Integer, Duration>> runTimes, ReportFormat format){
        this.format = format;
        this.runTimes = runTimes;
    }

    private void createPDFReport() {
        // TODO: Yet To Implement
    }

    private void createTextReport() {

    }

    private void createHTMLReport() {
        // TODO: Yet To Implement
    }

    public void createReport(){
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
