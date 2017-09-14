package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.ReportFormat;

/* This class is used to build a report in different formats after
the benchmark is executed successfully
 */
public class ReportBuilder {

    private ReportFormat format;

    public ReportBuilder(ReportFormat format){
        this.format = format;
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
