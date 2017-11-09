package edu.uci.ics.tippers.data.griddb;

import com.toshiba.mwcloud.gs.*;
import edu.uci.ics.tippers.common.DataFiles;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.BigJsonReader;
import edu.uci.ics.tippers.connection.griddb.StoreManager;
import edu.uci.ics.tippers.data.BaseDataUploader;
import edu.uci.ics.tippers.data.griddb.mappings.GridDBDataMapping1;
import edu.uci.ics.tippers.data.griddb.mappings.GridDBDataMapping2;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.model.observation.Observation;

import java.time.Duration;
import java.time.Instant;

public class GridDBDataUploader extends BaseDataUploader {

    private GridStore gridStore;
    private GridDBBaseDataMapping dataMapping;

    public GridDBDataUploader(int mapping, String dataDir) throws BenchmarkException {
        super(mapping, dataDir);
        gridStore = StoreManager.getInstance().getGridStore();
        switch (mapping) {
            case 1:
                dataMapping = new GridDBDataMapping1(gridStore, dataDir);
                break;
            case 2:
                dataMapping = new GridDBDataMapping2(gridStore, dataDir);
                break;
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Database getDatabase() {
        return Database.GRIDDB;
    }

    @Override
    public Duration addAllData() throws BenchmarkException {
        try {
            Instant start = Instant.now();
            dataMapping.addAll();
            Instant end = Instant.now();
            return Duration.between(start, end);
        } catch (GSException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Inserting Data");
        }
    }

    @Override
    public void addInfrastructureData() {

    }

    @Override
    public void addUserData() {

    }

    @Override
    public void addSensorData() {

    }

    @Override
    public void addDeviceData() {

    }

    @Override
    public void addObservationData() {

    }

    @Override
    public void virtualSensorData() {

    }

    @Override
    public void addSemanticObservationData() {

    }

    @Override
    public Duration insertPerformance() throws BenchmarkException {
        Instant start = Instant.now();
        switch (mapping) {
            case 1:
                Row row;
                BigJsonReader<Observation> reader = new BigJsonReader<>(dataDir + DataFiles.INSERT_TEST.getPath(),
                        Observation.class);
                Observation obs = null;
                try {
                    while ((obs = reader.readNext()) != null) {
                        String collectionName = Constants.GRIDDB_OBS_PREFIX + obs.getSensor().getId();

                        TimeSeries<Row> timeSeries = null;

                        timeSeries = gridStore.getTimeSeries(collectionName);


                        row = timeSeries.createRow();
                        row.setValue(0, obs.getTimeStamp());
                        row.setValue(1, obs.getId());

                        if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                            row.setValue(2, obs.getPayload().get("temperature").getAsInt());
                        } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                            row.setValue(2, obs.getPayload().get("clientId").getAsString());
                        } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                            row.setValue(2, obs.getPayload().get("currentMilliWatts").getAsInt());
                            row.setValue(3, obs.getPayload().get("onTodaySeconds").getAsInt());
                        }
                        timeSeries.put(row);
                    }
                } catch(GSException e){
                    e.printStackTrace();
                }
                break;
            case 2:
                reader = new BigJsonReader<>(dataDir + DataFiles.INSERT_TEST.getPath(),
                        Observation.class);

                try {
                    while ((obs = reader.readNext()) != null) {
                        String collectionName = null;
                        if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                            collectionName = "WiFiAPObservation";
                        } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                            collectionName = "WeMoObservation";
                        } else if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                            collectionName = "ThermometerObservation";
                        }

                        Collection<String, Row> collection = gridStore.getCollection(collectionName);


                        row = collection.createRow();
                        row.setValue(0, obs.getTimeStamp());
                        row.setValue(1, obs.getId());
                        row.setValue(2, obs.getSensor().getId());

                        if (obs.getSensor().getType_().getId().equals("Thermometer")) {
                            row.setValue(3, obs.getPayload().get("temperature").getAsInt());
                        } else if (obs.getSensor().getType_().getId().equals("WiFiAP")) {
                            row.setValue(3, obs.getPayload().get("clientId").getAsString());
                        } else if (obs.getSensor().getType_().getId().equals("WeMo")) {
                            row.setValue(3, obs.getPayload().get("currentMilliWatts").getAsInt());
                            row.setValue(4, obs.getPayload().get("onTodaySeconds").getAsInt());
                        }
                        collection.put(row);
                    }
                } catch (GSException e) {
                    e.printStackTrace();
                }
                break;
            default:
                throw new BenchmarkException("No Such Mapping");

        }
        Instant end = Instant.now();
        return Duration.between(start, end);
    }
}
