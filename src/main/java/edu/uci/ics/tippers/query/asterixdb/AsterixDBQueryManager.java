package edu.uci.ics.tippers.query.asterixdb;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.common.util.Helper;
import edu.uci.ics.tippers.connection.asterixdb.AsterixDBConnectionManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class AsterixDBQueryManager extends BaseQueryManager{

    private static final Logger LOGGER = Logger.getLogger(AsterixDBQueryManager.class);

    private AsterixDBConnectionManager connectionManager;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static SimpleDateFormat dateOnlyFormat = new SimpleDateFormat("yyyy-MM-dd");

    public AsterixDBQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout){
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        connectionManager = AsterixDBConnectionManager.getInstance();
    }

    @Override
    public Database getDatabase() {
        return Database.ASTERIXDB;
    }

    @Override
    public void cleanUp() {

    }

    private Duration runTimedQuery (String query, int queryNum) throws BenchmarkException {
       
        LOGGER.info(String.format("Running Query %s", queryNum));
        LOGGER.info(query);

	    Instant startTime = Instant.now();
        HttpResponse response = connectionManager.sendQuery(query);
        Instant endTime = Instant.now();

	
        if (writeOutput) {
            // TODO: Write To File
            try {
                RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping,
                        Helper.getFileFromQuery(queryNum));
                writer.writeString(EntityUtils.toString(response.getEntity()));
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new BenchmarkException("Error writing output to file");
            }
        }
        return Duration.between(startTime, endTime);
    }

    private Duration explainQuery (String query, int queryNum) throws BenchmarkException {

        LOGGER.info(String.format("Running Expalin Query %s", queryNum));
        LOGGER.info(query);

        Instant startTime = Instant.now();
        HttpResponse response = connectionManager.sendQuery("EXPLAIN "+ query);
        Instant endTime = Instant.now();

        try {
            RowWriter<String> writer = new RowWriter<>(outputDir+"/explains/", getDatabase(), mapping,
                    Helper.getFileFromQuery(queryNum));
            writer.writeString(EntityUtils.toString(response.getEntity()));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error writing output to file");
        }

        return Duration.between(startTime, endTime);
    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                return runTimedQuery(
                    String.format("SELECT name FROM Sensor WHERE id = \"%s\";", sensorId), 1
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {

        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT s.id, s.name FROM Sensor s WHERE s.type_.name=\"%s\" AND "
                                        + "(SOME e IN s.coverage SATISFIES e.id IN {{"
                                        + locationIds.stream().map(e -> "\"" + e + "\"" ).collect(Collectors.joining(","))
                                        + "}});",
                                sensorTypeName), 2
                );
            case 2:
                return runTimedQuery(
                        String.format("SELECT s.id, s.name FROM Sensor s WHERE s.type_.name=\"%s\" AND "
                                        + "(SOME e IN s.coverage SATISFIES e IN {{"
                                        + locationIds.stream().map(e -> "\"" + e + "\"" ).collect(Collectors.joining(","))
                                        + "}});",
                                sensorTypeName), 2
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT timeStamp, sensor.id, payload FROM Observation WHERE sensor.id=\"%s\" "
                                + "AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\");",
                                sensorId, sdf.format(startTime), sdf.format(endTime)), 3
                );
            case 2:
                return runTimedQuery(
                        String.format("SELECT timeStamp, sensorId, payload FROM Observation WHERE sensorId=\"%s\" "
                                        + "AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\");",
                                sensorId, sdf.format(startTime), sdf.format(endTime)), 3
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT timeStamp, sensor.id, payload FROM Observation WHERE sensor.id IN {{ "
                                        + sensorIds.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(","))
                                        + " }} AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\");",
                                sdf.format(startTime), sdf.format(endTime)),  4
                );
            case 2:
                return runTimedQuery(
                        String.format("SELECT timeStamp, sensorId, payload FROM Observation WHERE sensorId IN {{ "
                                        + sensorIds.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(","))
                                        + " }} AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\");",
                                sdf.format(startTime), sdf.format(endTime)),  4
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                          Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT timeStamp, sensor.id, payload " +
                            "FROM Observation " +
                            "WHERE sensor.type_.name = \"%s\" AND timeStamp >= datetime(\"%s\") AND " +
                            "timeStamp <= datetime(\"%s\") " +
                            "AND payload.%s >= %s AND payload.%s <= %s",
                                sensorTypeName, sdf.format(startTime), sdf.format(endTime), payloadAttribute,
                                startPayloadValue, payloadAttribute, endPayloadValue), 5
                );
            case 2:
                return runTimedQuery(
                        String.format("SELECT obs.timeStamp, obs.sensorId, obs.payload " +
                                        "FROM Observation obs, Sensor sen " +
                                        "WHERE obs.sensorId = sen.id AND sen.type_.name = \"%s\" AND " +
                                        "obs.timeStamp >= datetime(\"%s\") AND " +
                                        "obs.timeStamp <= datetime(\"%s\") " +
                                        "AND obs.payload.%s >= %s AND obs.payload.%s <= %s",
                                sensorTypeName, sdf.format(startTime), sdf.format(endTime), payloadAttribute,
                                startPayloadValue, payloadAttribute, endPayloadValue), 5
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT obs.id , AVG(obs.count) FROM " +
                                "(SELECT sensor.id , get_date_from_datetime(timeStamp), count(*)  AS count " +
                                "FROM Observation " +
                                "WHERE sensor.id IN {{ " +
                                sensorIds.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(",")) +
                                " }} AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\") " +
                                "GROUP BY sensor.id, get_date_from_datetime(timeStamp)) AS obs GROUP BY obs.id",
                                sdf.format(startTime), sdf.format(endTime)), 6
                );
            case 2:
                return runTimedQuery(
                        String.format("SELECT obs.sensorId , AVG(obs.count) FROM " +
                                        "(SELECT sensorId , get_date_from_datetime(timeStamp), count(*)  AS count " +
                                        "FROM Observation " +
                                        "WHERE sensorId IN {{ " +
                                        sensorIds.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(",")) +
                                        " }} AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\") " +
                                        "GROUP BY sensorId, get_date_from_datetime(timeStamp)) AS obs GROUP BY obs.sensorId",
                                sdf.format(startTime), sdf.format(endTime)), 6
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT s1.semanticEntity.name " +
                                " FROM SemanticObservation s1, SemanticObservation s2 " +
                                " WHERE get_date_from_datetime(s1.timeStamp) = date(\"%s\") AND " +
                                " get_date_from_datetime(s2.timeStamp) = date(\"%s\") AND " +
                                " s1.type_.name = s2.type_.name AND s2.type_.name = \"presence\" AND " +
                                " s1.payload.location = \"%s\" AND s2.payload.location = \"%s\" " +
                                " AND s1.timeStamp < s2.timeStamp AND s1.semanticEntity.id = s2.semanticEntity.id",
                                dateOnlyFormat.format(date), dateOnlyFormat.format(date), startLocation, endLocation), 7
                );
            case 2:
                return runTimedQuery(
                        String.format("SELECT se.name " +
                                        " FROM SemanticObservation s1, SemanticObservation s2, User se, SemanticObservationType st" +
                                        " WHERE get_date_from_datetime(s1.timeStamp) = date(\"%s\") AND " +
                                        " get_date_from_datetime(s2.timeStamp) = date(\"%s\") AND " +
                                        " s1.typeId = s2.typeId AND s2.typeId = st.id AND st.name = \"presence\" AND " +
                                        " s1.payload.location = \"%s\" AND s2.payload.location = \"%s\" " +
                                        " AND s1.timeStamp < s2.timeStamp AND s1.semanticEntityId = s2.semanticEntityId " +
                                        " AND s2.semanticEntityId = se.id",
                                dateOnlyFormat.format(date), dateOnlyFormat.format(date), startLocation, endLocation), 7
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery8(String userId, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT s2.semanticEntity.name, s1.payload.location " +
                                        " FROM SemanticObservation s1, SemanticObservation s2 " +
                                        " WHERE get_date_from_datetime(s1.timeStamp) = date(\"%s\") AND " +
                                        " get_date_from_datetime(s2.timeStamp) = date(\"%s\") AND " +
                                        " s1.type_.name = s2.type_.name AND s2.type_.name = \"presence\" AND " +
                                        " s1.payload.location = s2.payload.location " +
                                        " AND s1.timeStamp = s2.timeStamp AND s1.semanticEntity.id = \"%s\" " +
                                        " AND s2.semanticEntity.id != s1.semanticEntity.id",
                                dateOnlyFormat.format(date), dateOnlyFormat.format(date), userId), 8
                );
            case 2:
                return runTimedQuery(
                        String.format("SELECT se.name, s1.payload.location " +
                                        " FROM SemanticObservation s1, SemanticObservation s2, User se, SemanticObservationType st" +
                                        " WHERE get_date_from_datetime(s1.timeStamp) = date(\"%s\") AND " +
                                        " get_date_from_datetime(s2.timeStamp) = date(\"%s\") AND " +
                                        " s1.typeId = s2.typeId AND s2.typeId = st.id AND st.name = \"presence\" AND " +
                                        " s1.payload.location = s2.payload.location " +
                                        " AND s1.timeStamp = s2.timeStamp AND s1.semanticEntityId = \"%s\" " +
                                        " AND s2.semanticEntityId = se.id AND s2.semanticEntityId != s1.semanticEntityId",
                                dateOnlyFormat.format(date), dateOnlyFormat.format(date), userId), 8
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery8WithSelectivity(String userId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                return runTimedQuery(
                        String.format("SELECT se.name, s1.payload.location " +
                                        " FROM SemanticObservation s1, SemanticObservation s2, User se, SemanticObservationType st" +
                                        " WHERE s1.timeStamp >= datetime(\"%s\") AND s1.timeStamp <= datetime(\"%s\") AND " +
                                        " s2.timeStamp = s1.timeStamp AND " +
                                        " s1.typeId = s2.typeId AND s2.typeId = st.id AND st.name = \"presence\" AND " +
                                        " s1.payload.location = s2.payload.location " +
                                        " AND s1.timeStamp = s2.timeStamp AND s1.semanticEntityId = \"%s\" " +
                                        " AND s2.semanticEntityId = se.id AND s2.semanticEntityId != s1.semanticEntityId",
                                sdf.format(startTime), sdf.format(endTime), userId), 8
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery9(String userId, String infraTypeName) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT AVG(groups.timeSpent) AS avgTimePerDay FROM " +
                                        " (SELECT get_date_from_datetime(so.timeStamp), count(*)*10  AS timeSpent " +
                                        " FROM SemanticObservation so, Infrastructure infra " +
                                        " WHERE so.type_.name=\"presence\" AND so.semanticEntity.id=\"%s\" " +
                                        " AND so.payload.location = infra.id AND infra.type_.name = \"%s\"" +
                                        " GROUP BY get_date_from_datetime(so.timeStamp))  AS groups ",
                                userId, infraTypeName), 9
                );
            case 2:
                return runTimedQuery(
                        String.format("SELECT AVG(groups.timeSpent) AS avgTimePerDay FROM " +
                                        " (SELECT get_date_from_datetime(so.timeStamp), count(*)*10  AS timeSpent " +
                                        " FROM SemanticObservation so, Infrastructure infra, SemanticObservationType st " +
                                        " WHERE so.typeId = st.id AND st.name = \"presence\" AND so.semanticEntityId=\"%s\" " +
                                        " AND so.payload.location = infra.id AND infra.type_.name = \"%s\"" +
                                        " GROUP BY get_date_from_datetime(so.timeStamp)) AS groups ",
                                userId, infraTypeName), 9
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration runQuery10(Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return runTimedQuery(
                        String.format("SELECT infra.name, (" +
                                        "SELECT so.timeStamp, so.payload.occupancy " +
                                        "FROM SemanticObservation so " +
                                        "WHERE so.timeStamp > datetime(\"%s\") AND so.timeStamp < datetime(\"%s\") " +
                                        "AND so.type_.name = \"occupancy\" AND so.semanticEntity.id = infra.id " +
                                        "ORDER BY so.semanticEntity.id, so.timeStamp) AS histogram " +
                                        "FROM Infrastructure infra", sdf.format(startTime), sdf.format(endTime)), 10
                );
            case 2:
                return runTimedQuery(
                        String.format("SELECT infra.name, (" +
                                "SELECT so.timeStamp, so.payload.occupancy " +
                                "FROM SemanticObservation so, SemanticObservationType st " +
                                "WHERE so.timeStamp > datetime(\"%s\") AND so.timeStamp < datetime(\"%s\") " +
                                "AND so.typeId = st.id AND st.name = \"occupancy\" AND so.semanticEntityId = infra.id " +
                                "ORDER BY so.semanticEntityId, so.timeStamp) AS histogram " +
                                "FROM Infrastructure infra", sdf.format(startTime), sdf.format(endTime)), 10
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration explainQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                return explainQuery(
                        String.format("SELECT name FROM Sensor WHERE id = \"%s\";", sensorId), 1
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {

        switch (mapping) {
            case 1:
                return explainQuery(
                        String.format("SELECT s.id, s.name FROM Sensor s WHERE s.type_.name=\"%s\" AND "
                                        + "(SOME e IN s.coverage SATISFIES e.id IN {{"
                                        + locationIds.stream().map(e -> "\"" + e + "\"" ).collect(Collectors.joining(","))
                                        + "}});",
                                sensorTypeName), 2
                );
            case 2:
                return explainQuery(
                        String.format("SELECT s.id, s.name FROM Sensor s WHERE s.type_.name=\"%s\" AND "
                                        + "(SOME e IN s.coverage SATISFIES e IN {{"
                                        + locationIds.stream().map(e -> "\"" + e + "\"" ).collect(Collectors.joining(","))
                                        + "}});",
                                sensorTypeName), 2
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration explainQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return explainQuery(
                        String.format("SELECT timeStamp, sensor.id, payload FROM Observation WHERE sensor.id=\"%s\" "
                                        + "AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\");",
                                sensorId, sdf.format(startTime), sdf.format(endTime)), 3
                );
            case 2:
                return explainQuery(
                        String.format("SELECT timeStamp, sensorId, payload FROM Observation WHERE sensorId=\"%s\" "
                                        + "AND timeStamp >= %s AND timeStamp <= %s;",
                                sensorId, startTime.getTime()*1000, endTime.getTime()*1000), 3
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return explainQuery(
                        String.format("SELECT timeStamp, sensor.id, payload FROM Observation WHERE sensor.id IN {{ "
                                        + sensorIds.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(","))
                                        + " }} AND timeStamp >= %s AND timeStamp <= %s;",
                                startTime.getTime()*1000, endTime.getTime()*1000),  4
                );
            case 2:
                return explainQuery(
                        String.format("SELECT timeStamp, sensorId, payload FROM Observation WHERE sensorId IN {{ "
                                        + sensorIds.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(","))
                                        + " }} AND timeStamp >= %s AND timeStamp <= %s;",
                                startTime.getTime()*1000, endTime.getTime()*1000),  4
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return explainQuery(
                        String.format("SELECT timeStamp, sensor.id, payload " +
                                        "FROM Observation " +
                                        "WHERE sensor.type_.name = \"%s\" AND timeStamp >= datetime(\"%s\") AND " +
                                        "timeStamp <= datetime(\"%s\") " +
                                        "AND payload.%s >= %s AND payload.%s <= %s",
                                sensorTypeName, sdf.format(startTime), sdf.format(endTime), payloadAttribute,
                                startPayloadValue, payloadAttribute, endPayloadValue), 5
                );
            case 2:
                return explainQuery(
                        String.format("SELECT obs.timeStamp, obs.sensorId, obs.payload " +
                                        "FROM Observation obs, Sensor sen " +
                                        "WHERE obs.sensorId = sen.id AND sen.type_.name = \"%s\" AND " +
                                        "obs.timeStamp >= datetime(\"%s\") AND " +
                                        "obs.timeStamp <= datetime(\"%s\") " +
                                        "AND obs.payload.%s >= %s AND obs.payload.%s <= %s",
                                sensorTypeName, sdf.format(startTime), sdf.format(endTime), payloadAttribute,
                                startPayloadValue, payloadAttribute, endPayloadValue), 5
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration explainQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return explainQuery(
                        String.format("SELECT obs.id , AVG(obs.count) FROM " +
                                        "(SELECT sensor.id , get_date_from_datetime(timeStamp), count(*)  AS count " +
                                        "FROM Observation " +
                                        "WHERE sensor.id IN {{ " +
                                        sensorIds.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(",")) +
                                        " }} AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\") " +
                                        "GROUP BY sensor.id, get_date_from_datetime(timeStamp)) AS obs GROUP BY obs.id",
                                sdf.format(startTime), sdf.format(endTime)), 6
                );
            case 2:
                return explainQuery(
                        String.format("SELECT obs.sensorId , AVG(obs.count) FROM " +
                                        "(SELECT sensorId , get_date_from_datetime(timeStamp), count(*)  AS count " +
                                        "FROM Observation " +
                                        "WHERE sensorId IN {{ " +
                                        sensorIds.stream().map(e -> "\"" + e + "\"").collect(Collectors.joining(",")) +
                                        " }} AND timeStamp >= datetime(\"%s\") AND timeStamp <= datetime(\"%s\") " +
                                        "GROUP BY sensorId, get_date_from_datetime(timeStamp)) AS obs GROUP BY obs.sensorId",
                                sdf.format(startTime), sdf.format(endTime)), 6
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration explainQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return explainQuery(
                        String.format("SELECT s1.semanticEntity.name " +
                                        " FROM SemanticObservation s1, SemanticObservation s2 " +
                                        " WHERE get_date_from_datetime(s1.timeStamp) = date(\"%s\") AND " +
                                        " get_date_from_datetime(s2.timeStamp) = date(\"%s\") AND " +
                                        " s1.type_.name = s2.type_.name AND s2.type_.name = \"presence\" AND " +
                                        " s1.payload.location = \"%s\" AND s2.payload.location = \"%s\" " +
                                        " AND s1.timeStamp < s2.timeStamp AND s1.semanticEntity.id = s2.semanticEntity.id",
                                dateOnlyFormat.format(date), dateOnlyFormat.format(date), startLocation, endLocation), 7
                );
            case 2:
                return explainQuery(
                        String.format("SELECT se.name " +
                                        " FROM SemanticObservation s1, SemanticObservation s2, User se, SemanticObservationType st" +
                                        " WHERE get_date_from_datetime(s1.timeStamp) = date(\"%s\") AND " +
                                        " get_date_from_datetime(s2.timeStamp) = date(\"%s\") AND " +
                                        " s1.typeId = s2.typeId AND s2.typeId = st.id AND st.name = \"presence\" AND " +
                                        " s1.payload.location = \"%s\" AND s2.payload.location = \"%s\" " +
                                        " AND s1.timeStamp < s2.timeStamp AND s1.semanticEntityId = s2.semanticEntityId " +
                                        " AND s2.semanticEntityId = se.id",
                                dateOnlyFormat.format(date), dateOnlyFormat.format(date), startLocation, endLocation), 7
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration explainQuery8(String userId, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return explainQuery(
                        String.format("SELECT s2.semanticEntity.name, s1.payload.location " +
                                        " FROM SemanticObservation s1, SemanticObservation s2 " +
                                        " WHERE get_date_from_datetime(s1.timeStamp) = date(\"%s\") AND " +
                                        " get_date_from_datetime(s2.timeStamp) = date(\"%s\") AND " +
                                        " s1.type_.name = s2.type_.name AND s2.type_.name = \"presence\" AND " +
                                        " s1.payload.location = s2.payload.location " +
                                        " AND s1.timeStamp = s2.timeStamp AND s1.semanticEntity.id = \"%s\" " +
                                        " AND s2.semanticEntity.id != s1.semanticEntity.id",
                                dateOnlyFormat.format(date), dateOnlyFormat.format(date), userId), 8
                );
            case 2:
                return explainQuery(
                        String.format("SELECT se.name, s1.payload.location " +
                                        " FROM SemanticObservation s1, SemanticObservation s2, User se, SemanticObservationType st" +
                                        " WHERE get_date_from_datetime(s1.timeStamp) = date(\"%s\") AND " +
                                        " get_date_from_datetime(s2.timeStamp) = date(\"%s\") AND " +
                                        " s1.typeId = s2.typeId AND s2.typeId = st.id AND st.name = \"presence\" AND " +
                                        " s1.payload.location = s2.payload.location " +
                                        " AND s1.timeStamp = s2.timeStamp AND s1.semanticEntityId = \"%s\" " +
                                        " AND s2.semanticEntityId = se.id AND s2.semanticEntityId != s1.semanticEntityId",
                                dateOnlyFormat.format(date), dateOnlyFormat.format(date), userId), 8
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }
    @Override
    public Duration explainQuery9(String userId, String infraTypeName) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return explainQuery(
                        String.format("SELECT AVG(groups.timeSpent) AS avgTimePerDay FROM " +
                                        " (SELECT get_date_from_datetime(so.timeStamp), count(*)*10  AS timeSpent " +
                                        " FROM SemanticObservation so, Infrastructure infra " +
                                        " WHERE so.type_.name=\"presence\" AND so.semanticEntity.id=\"%s\" " +
                                        " AND so.payload.location = infra.id AND infra.type_.name = \"%s\"" +
                                        " GROUP BY get_date_from_datetime(so.timeStamp))  AS groups ",
                                userId, infraTypeName), 9
                );
            case 2:
                return explainQuery(
                        String.format("SELECT AVG(groups.timeSpent) AS avgTimePerDay FROM " +
                                        " (SELECT get_date_from_datetime(so.timeStamp), count(*)*10  AS timeSpent " +
                                        " FROM SemanticObservation so, Infrastructure infra, SemanticObservationType st " +
                                        " WHERE so.typeId = st.id AND st.name = \"presence\" AND so.semanticEntityId=\"%s\" " +
                                        " AND so.payload.location = infra.id AND infra.type_.name = \"%s\"" +
                                        " GROUP BY get_date_from_datetime(so.timeStamp)) AS groups ",
                                userId, infraTypeName), 9
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }

    @Override
    public Duration explainQuery10(Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                return explainQuery(
                        String.format("SELECT infra.name, (" +
                                "SELECT so.timeStamp, so.payload.occupancy " +
                                "FROM SemanticObservation so " +
                                "WHERE so.timeStamp > datetime(\"%s\") AND so.timeStamp < datetime(\"%s\") " +
                                "AND so.type_.name = \"occupancy\" AND so.semanticEntity.id = infra.id " +
                                "ORDER BY so.semanticEntity.id, so.timeStamp) AS histogram " +
                                "FROM Infrastructure infra", sdf.format(startTime), sdf.format(endTime)), 10
                );
            case 2:
                return explainQuery(
                        String.format("SELECT infra.name, (" +
                                "SELECT so.timeStamp, so.payload.occupancy " +
                                "FROM SemanticObservation so, SemanticObservationType st " +
                                "WHERE so.timeStamp > datetime(\"%s\") AND so.timeStamp < datetime(\"%s\") " +
                                "AND so.typeId = st.id AND st.name = \"occupancy\" AND so.semanticEntityId = infra.id " +
                                "ORDER BY so.semanticEntityId, so.timeStamp) AS histogram " +
                                "FROM Infrastructure infra", sdf.format(startTime), sdf.format(endTime)), 10
                );
            default:
                throw new BenchmarkException("No Such Mapping");
        }

    }
}
