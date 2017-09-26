package edu.uci.ics.tippers.scaler;

import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.scaler.data.DataConfiguration;
import edu.uci.ics.tippers.scaler.data.ScaleData;
import edu.uci.ics.tippers.scaler.query.QueryConfiguration;
import edu.uci.ics.tippers.scaler.query.ScaleQuery;

import java.io.IOException;

public class Scale {

    boolean scaleQueries;
    boolean scaleData;
    String dataDir;

    public Scale(boolean scaleQueries, boolean scaleData, String dataDir) {
        this.scaleData = scaleData;
        this.scaleQueries = scaleQueries;
        this.dataDir = dataDir;
    }

    public void scaleDataAndQueries() {
        if (scaleData) {
            try {
                DataConfiguration dataConf = new DataConfiguration(dataDir);
                ScaleData dataScaler = new ScaleData(dataConf);
                dataScaler.generateData();
            } catch (IOException e) {
                e.printStackTrace();
                throw new BenchmarkException("Error Reading Configuration File");
            }
        } else {
            // TODO: Copy Data Files
        }

        if (scaleQueries) {
            try {
                QueryConfiguration queryConf = new QueryConfiguration();
                ScaleQuery queryScaler = new ScaleQuery(queryConf);
                queryScaler.generateQueries();
            } catch (IOException e) {
                e.printStackTrace();
                throw new BenchmarkException("Error Reading Configuration File");
            }
        } else {
            // TODO: Copy Query Files
        }


    }

}
