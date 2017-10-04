package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.exception.BenchmarkException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/* Class responsible for starting up and stopping database systems

 */
public class DBMSManager {

    private static String scriptsDir;
    private static final String startUp = "startup_script.sh";
    private static final String stop = "stop_script.sh";

    public DBMSManager(String scriptsDir) {
        this.scriptsDir = scriptsDir;
    }

    private static void printLines(InputStream ins) throws Exception {
        String line = null;
        BufferedReader in = new BufferedReader(
                new InputStreamReader(ins));
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
    }

    public void runShellScript(String scriptFile) throws BenchmarkException {
        ProcessBuilder pb = new ProcessBuilder(scriptFile);
        try {
            Process p = pb.start();
            printLines(p.getInputStream());
            printLines(p.getErrorStream());
            p.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
            throw new BenchmarkException("Error starting up Database Servers");
        }
    }

    public void startAllServers() throws BenchmarkException {
        runShellScript(scriptsDir + startUp);
    }

    public void stopAllServers() throws BenchmarkException {
        runShellScript(scriptsDir + stop);

    }

    public void startServer(Database database) {
        runShellScript(String.format("%sstart/%s.sh",this.scriptsDir, database.getName()));
    }

    public void stopServer(Database database) {
        runShellScript(String.format("%sstop/%s.sh",this.scriptsDir, database.getName()));
    }


}
