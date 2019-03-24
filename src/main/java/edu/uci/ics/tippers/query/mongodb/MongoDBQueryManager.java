package edu.uci.ics.tippers.query.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.mongodb.DBManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;
import org.bson.*;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static edu.uci.ics.tippers.common.util.Helper.getFileFromQuery;


public class MongoDBQueryManager extends BaseQueryManager{

    private MongoDatabase database;
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public MongoDBQueryManager(int mapping, String queriesDir, String outputDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, outputDir, writeOutput, timeout);
        database = DBManager.getInstance().getDatabase();
    }

    @Override
    public Database getDatabase() {
        return Database.MONGODB;
    }

    @Override
    public void cleanUp() {

    }

    private void getResults(MongoIterable<Document> iterable, int queryNum ) {
        try {
            RowWriter<String> writer = new RowWriter<>(outputDir, getDatabase(), mapping, getFileFromQuery(queryNum));
            iterable.forEach((Consumer<? super Document>) e -> {
                if (writeOutput) {
                    try {
                        writer.writeString(e.toJson());
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            });
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Writing Output To File");
        }
    }

    private void getExplainResults(MongoIterable<Document> iterable, int queryNum ) {
        try {
            RowWriter<String> writer = new RowWriter<>(outputDir+"/explains/", getDatabase(), mapping, getFileFromQuery(queryNum));
            iterable.forEach((Consumer<? super Document>) e -> {
                try {
                    writer.writeString(e.toJson());
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            });
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Writing Output To File");
        }
    }

    private void writeDocumentToFile(Document document, int queryNum ) {
        try {
            RowWriter<String> writer = new RowWriter<>(outputDir+"/explains/", getDatabase(), mapping, getFileFromQuery(queryNum));
                try {
                    writer.writeString(document.toJson());
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Writing Output To File");
        }
    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Sensor");
                MongoIterable<Document> iterable = collection.find(eq("_id", sensorId))
                        .projection(new Document("name", 1).append("_id", 0));

                getResults(iterable, 1);
                Instant end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Sensor");
                MongoIterable<Document> iterable = collection.find(and(
                        eq("type_.name", sensorTypeName),
                        in("coverage.id", locationIds)))
                        .projection(new Document("name", 1)
                                .append("_id", 1));

                getResults(iterable, 2);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("Sensor");
                iterable = collection.find(and(
                        eq("type_.name", sensorTypeName),
                        in("coverage", locationIds)))
                        .projection(new Document("name", 1)
                                .append("_id", 1));

                getResults(iterable, 2);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Observation");
                MongoIterable<Document> iterable = collection.find(
                        and(
                        eq("sensor.id", sensorId),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("payload", 1)
                                .append("sensor.id", 1));

                getResults(iterable, 3);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("Observation");
                iterable = collection.find(
                        and(
                                eq("sensorId", sensorId),
                                gt("timeStamp", startTime),
                                lt("timeStamp", endTime)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("payload", 1)
                                .append("sensorId", 1));

                getResults(iterable, 3);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Observation");
                MongoIterable<Document> iterable = collection.find(
                        and(
                                in("sensor.id", sensorIds),
                                gt("timeStamp", startTime),
                                lt("timeStamp", endTime)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("sensor.id", 1)
                                .append("payload.temperature", 1));

                getResults(iterable, 4);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("Observation");
                iterable = collection.find(
                        and(
                                in("sensorId", sensorIds),
                                gt("timeStamp", startTime),
                                lt("timeStamp", endTime)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("sensorId", 1)
                                .append("payload", 1));

                getResults(iterable, 4);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Observation");
                MongoIterable<Document> iterable = collection.find(
                        and(
                                eq("sensor.type_.name", sensorTypeName),
                                gt("timeStamp", startTime),
                                lt("timeStamp", endTime),
                                gt(String.format("payload.%s", payloadAttribute), startPayloadValue),
                                lt(String.format("payload.%s", payloadAttribute), endPayloadValue)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("sensor.id", 1)
                                .append("payload", 1));

                getResults(iterable, 5);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("Observation");

                Bson lookUp = lookup("Sensor", "sensorId", "_id", "sensors");

                Bson match = match(and(
                        eq("sensors.type_.name", sensorTypeName),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        gte(String.format("payload.%s", payloadAttribute), startPayloadValue),
                        lte(String.format("payload.%s", payloadAttribute), endPayloadValue)
                ));

                Bson project = project(
                        fields(
                                excludeId(),
                                include("sensorId", "payload", "timeStamp")
                        )
                );

                iterable = collection.aggregate(Arrays.asList(lookUp, match, project));

                getResults(iterable, 5);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Observation");

                Bson match = match(and(
                        in("sensor.id", sensorIds),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime)));

                Bson project = project(
                        fields(
                                excludeId(),
                                include("sensor.id"),
                                computed(
                                        "date",
                                        new Document("$dateToString", new Document("format", "%Y-%m-%d")
                                                .append("date", "$timeStamp"))
                                )
                        )
                );

                Bson group1 = group(new Document("sensorId", "$sensor.id").append("date", "$date"),
                        sum("count", 1));
                Bson group2 = group(new Document("sensorId", "$_id.sensorId"), avg("averagePerDay", "$count"));

                MongoIterable<Document> iterable = collection.aggregate(Arrays.asList(match, project, group1, group2));

                getResults(iterable, 6);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("Observation");

                match = match(and(
                        in("sensorId", sensorIds),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime)));

                project = project(
                        fields(
                                excludeId(),
                                include("sensorId"),
                                computed(
                                        "date",
                                        new Document("$dateToString", new Document("format", "%Y-%m-%d")
                                                .append("date", "$timeStamp"))
                                )
                        )
                );

                group1 = group(new Document("sensorId", "$sensorId").append("date", "$date"),
                        sum("count", 1));
                group2 = group(new Document("sensorId", "$_id.sensorId"), avg("averagePerDay", "$count"));

                iterable = collection.aggregate(Arrays.asList(match, project, group1, group2));

                getResults(iterable, 6);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        /*switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection collection = database.getCollection("SemanticObservation");
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                Bson match1 = match(and(
                        eq("type_.name", "presence"),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("payload.location", startLocation)
                ));

                Bson lookUp = lookup("SemanticObservation", "semanticEntity.id",
                        "semanticEntity.id", "semantics");

                Bson unwind = unwind("$semantics");

                Bson project1 = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semantics"),
                                include("semanticEntity"),
                                include("payload"),
                                computed("timeCheck",
                                        new Document("$gt", Arrays.asList("$semantics.timeStamp", "$timeStamp")))
                        )
                );

                Bson match2 = match(and(
                        eq("semantics.type_.name", "presence"),
                        lt("semantics.timeStamp", endTime),
                        eq("semantics.payload.location", endLocation),
                        eq("timeCheck", true)
                ));

                Bson project2 = project(
                        fields(
                                include("semanticEntity.name")
                        )
                );

                MongoIterable iterable = collection.aggregate(Arrays.asList(match1, lookUp, unwind, project1, match2, project2));

                getResults(iterable, 7);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("SemanticObservation");
                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();

                Bson lookUp1 = lookup("SemanticObservationType", "typeId", "_id", "type_");

                match1 = match(and(
                        eq("type_.name", "presence"),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("payload.location", startLocation)
                ));

                Bson lookUp2 = lookup("SemanticObservation", "semanticEntityId",
                        "semanticEntityId", "semantics");

                unwind = unwind("$semantics");

                project1 = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semantics"),
                                include("semanticEntityId"),
                                include("payload"),
                                computed("timeCheck",
                                        new Document("$gt", Arrays.asList("$semantics.timeStamp", "$timeStamp"))),
                                computed("typeCheck",
                                        new Document("$eq", Arrays.asList("$semantics.typeId", "$typeId")))
                        )
                );

                match2 = match(and(
                        lt("semantics.timeStamp", endTime),
                        eq("semantics.payload.location", endLocation),
                        eq("timeCheck", true),
                        eq("typeCheck", true)
                ));

                Bson lookUp3 = lookup("User", "semanticEntityId",
                        "_id", "semanticEntity");

                Bson unwind2 = unwind("$semanticEntity");

                project2 = project(
                        fields(
                                include("semanticEntity.name")
                        )
                );

                iterable = collection.aggregate(Arrays.asList(lookUp1, match1, lookUp2, unwind, project1,
                        match2, lookUp3, unwind2, project2));

                getResults(iterable, 7);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }*/
        return Constants.MAX_DURATION;
    }

    public Duration runQuery7AppJoin(String startLocation, String endLocation, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                Instant start = Instant.now();

                MongoCollection collection = database.getCollection("SemanticObservation");
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                Bson lookUp1 = lookup("SemanticObservationType", "typeId", "_id", "type_");

                Bson match1 = match(and(
                        eq("type_.name", "presence"),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("payload.location", startLocation)
                ));

                Bson lookUp2 = lookup("SemanticObservation", "semanticEntityId",
                        "semanticEntityId", "semantics");

                Bson unwind = unwind("$semantics");

                Bson project1 = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semantics"),
                                include("semanticEntityId"),
                                include("payload"),
                                computed("timeCheck",
                                        new Document("$gt", Arrays.asList("$semantics.timeStamp", "$timeStamp"))),
                                computed("typeCheck",
                                        new Document("$eq", Arrays.asList("$semantics.typeId", "$typeId")))
                        )
                );

                Bson match2 = match(and(
                        lt("semantics.timeStamp", endTime),
                        eq("semantics.payload.location", endLocation),
                        eq("timeCheck", true),
                        eq("typeCheck", true)
                ));

                Bson lookUp3 = lookup("User", "semanticEntityId",
                        "_id", "semanticEntity");

                Bson unwind2 = unwind("$semanticEntity");

                Bson project2 = project(
                        fields(
                                include("semanticEntity.name")
                        )
                );

                MongoIterable iterable = collection.aggregate(Arrays.asList(lookUp1, match1, lookUp2, unwind, project1,
                        match2, lookUp3, unwind2, project2));

                getResults(iterable, 7);

                Instant end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery8(String userId, Date date) throws BenchmarkException {
        /*switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection collection = database.getCollection("SemanticObservation");
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                Bson match1 = match(and(
                        eq("type_.name", "presence"),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("semanticEntity.id", userId)
                ));

                Bson lookUp = lookup("SemanticObservation", "semanticEntity.id",
                        "semanticEntity.id", "semantics");

                Bson unwind = unwind("$semantics");

                Bson project1 = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semantics"),
                                include("semanticEntity"),
                                include("payload"),
                                computed("timeCheck",
                                        new Document("$eq", Arrays.asList("$semantics.timeStamp", "$timeStamp"))),
                                computed("placeCheck",
                                        new Document("$eq", Arrays.asList("$semantics.payload.location", "$payload.location")))
                        )
                );

                Bson match2 = match(and(
                        eq("semantics.type_.name", "presence"),
                        lt("semantics.timeStamp", endTime),
                        eq("timeCheck", true),
                        eq("timeCheck", true)
                ));

                Bson project2 = project(
                        fields(
                                include("semantics.semanticEntity.name"),
                                include("payload.location")
                        )
                );

                MongoIterable iterable = collection.aggregate(Arrays.asList(match1, lookUp, unwind, project1, match2, project2));

                getResults(iterable, 7);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("SemanticObservation");
                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();

                Bson lookUp1 = lookup("SemanticObservationType", "typeId", "_id", "type_");

                match1 = match(and(
                        eq("type_.name", "presence"),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("semanticEntityId", userId)
                ));

                Bson lookUp2 = lookup("SemanticObservation", "payload.location",
                        "payload.location", "semantics");

                unwind = unwind("$semantics");

                project1 = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semantics"),
                                include("semanticEntityId"),
                                include("payload"),
                                computed("timeCheck",
                                        new Document("$eq", Arrays.asList("$semantics.timeStamp", "$timeStamp"))),
                                computed("typeCheck",
                                        new Document("$eq", Arrays.asList("$semantics.typeId", "$typeId")))
                        )
                );

                match2 = match(and(
                        ne("semantics.semanticEntityId", userId),
                        lt("semantics.timeStamp", endTime),
                        eq("timeCheck", true),
                        eq("typeCheck", true)
                ));

                Bson lookUp3 = lookup("User", "semantics.semanticEntityId",
                        "_id", "semanticEntity");

                Bson unwind2 = unwind("$semanticEntity");

                project2 = project(
                        fields(
                                include("semanticEntity.name"),
                                include("payload.location")
                        )
                );

                iterable = collection.aggregate(Arrays.asList(lookUp1, match1, lookUp2, unwind, project1,
                        match2, lookUp3, unwind2, project2));

                getResults(iterable, 8);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }*/
        return Constants.MAX_DURATION;
    }

    public Duration runQuery8AppJoin(String userId, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                Instant start = Instant.now();

                MongoCollection collection = database.getCollection("SemanticObservation");
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();


                Bson lookUp1 = lookup("SemanticObservationType", "typeId", "_id", "type_");

                Bson match1 = match(and(
                        eq("type_.name", "presence"),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("semanticEntityId", userId)
                ));

                Bson lookUp2 = lookup("SemanticObservation", "payload.location",
                        "payload.location", "semantics");

                Bson unwind = unwind("$semantics");

                Bson project1 = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semantics"),
                                include("semanticEntityId"),
                                include("payload"),
                                computed("timeCheck",
                                        new Document("$eq", Arrays.asList("$semantics.timeStamp", "$timeStamp"))),
                                computed("typeCheck",
                                        new Document("$eq", Arrays.asList("$semantics.typeId", "$typeId")))
                        )
                );

                Bson match2 = match(and(
                        ne("semantics.semanticEntityId", userId),
                        lt("semantics.timeStamp", endTime),
                        eq("timeCheck", true),
                        eq("typeCheck", true)
                ));

                Bson lookUp3 = lookup("User", "semantics.semanticEntityId",
                        "_id", "semanticEntity");

                Bson unwind2 = unwind("$semanticEntity");

                Bson project2 = project(
                        fields(
                                include("semanticEntity.name"),
                                include("payload.location")
                        )
                );

                MongoIterable iterable = collection.aggregate(Arrays.asList(lookUp1, match1, lookUp2, unwind, project1,
                        match2, lookUp3, unwind2, project2));

                getResults(iterable, 8);

                Instant end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery8WithSelectivity(String userId, Date startTime, Date endTime) throws BenchmarkException {
        return null;
    }

    @Override
    public Duration runQuery9(String userId, String infraTypeName) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("SemanticObservation");

                Bson lookUp1 = lookup("Infrastructure", "payload.location", "_id", "infra");

                Bson unwind = unwind("$infra");

                Bson match = match(and(
                        eq("infra.type_.name", infraTypeName),
                        eq("semanticEntity.id", userId)));

                Bson project = project(
                        fields(
                                excludeId(),
                                computed(
                                        "date",
                                        new Document("$dateToString", new Document("format", "%Y-%m-%d")
                                                .append("date", "$timeStamp"))
                                )
                        )
                );

                Bson group1 = group(new Document("date", "$date"), sum("count", 1));
                Bson group2 = group(null, avg("averageMinsPerDay", "$count"));

                MongoIterable<Document> iterable = collection.aggregate(Arrays.asList(lookUp1, unwind, match, project,
                        group1, group2));

                getResults(iterable, 9);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("SemanticObservation");

                lookUp1 = lookup("Infrastructure", "payload.location", "_id", "infra");

                unwind = unwind("$infra");

                match = match(and(
                        eq("infra.type_.name", infraTypeName),
                        eq("semanticEntityId", userId)));

                project = project(
                        fields(
                                excludeId(),
                                computed(
                                        "date",
                                        new Document("$dateToString", new Document("format", "%Y-%m-%d")
                                                .append("date", "$timeStamp"))
                                )
                        )
                );

                group1 = group(new Document("date", "$date"), sum("count", 10));

                group2 = group(null, avg("averageMinsPerDay", "$count"));

                iterable = collection.aggregate(Arrays.asList(lookUp1, unwind, match, project, group1, group2));

                getResults(iterable, 9);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery10(Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("SemanticObservation");

                Bson match = match(and(
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("type_.name", "occupancy")
                ));

                Bson sort = sort(new Document("semanticEntity.id", 1).append("timeStamp", 1));

                Bson project = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semanticEntity.name"),
                                include("payload.occupancy")
                        )
                );

                MongoIterable<Document> iterable = collection.aggregate(Arrays.asList(match, sort, project)).allowDiskUse(true);

                getResults(iterable, 10);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("SemanticObservation");

                Bson lookUp1 = lookup("SemanticObservationType", "typeId", "_id", "type_");

                match = match(and(
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("type_.name", "occupancy")
                ));



                sort = sort(new Document("semanticEntityId", 1).append("timeStamp", 1));

                Bson lookUp2 = lookup("Infrastructure", "semanticEntityId", "_id", "infra");

                Bson unwind = unwind("$infra");

                project = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("infra.name"),
                                include("payload.occupancy")
                        )
                );

                iterable = collection.aggregate(Arrays.asList(lookUp1, match, sort, lookUp2, unwind, project)).allowDiskUse(true);

                getResults(iterable, 10);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }



    @Override
    public Duration explainQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Sensor");
                MongoIterable<Document> iterable = collection.find(eq("_id", sensorId))
                        .projection(new Document("name", 1).append("_id", 0)).modifiers(new Document("$explain", true));

                getExplainResults(iterable, 1);
                Instant end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery2(String sensorTypeName, List<String> locationIds) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Sensor");
                MongoIterable<Document> iterable = collection.find(and(
                        eq("type_.name", sensorTypeName),
                        in("coverage.id", locationIds)))
                        .projection(new Document("name", 1)
                                .append("_id", 1)).modifiers(new Document("$explain", true));

                getExplainResults(iterable, 2);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("Sensor");
                iterable = collection.find(and(
                        eq("type_.name", sensorTypeName),
                        in("coverage", locationIds)))
                        .projection(new Document("name", 1)
                                .append("_id", 1)).modifiers(new Document("$explain", true));

                getExplainResults(iterable, 2);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery3(String sensorId, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Observation");
                MongoIterable<Document> iterable = collection.find(
                        and(
                                eq("sensor.id", sensorId),
                                gt("timeStamp", startTime),
                                lt("timeStamp", endTime)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("payload", 1)
                                .append("sensor.id", 1)).modifiers(new Document("$explain", true));

                getExplainResults(iterable, 3);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("Observation");
                iterable = collection.find(
                        and(
                                eq("sensorId", sensorId),
                                gt("timeStamp", startTime),
                                lt("timeStamp", endTime)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("payload", 1)
                                .append("sensorId", 1)).modifiers(new Document("$explain", true));

                getExplainResults(iterable, 3);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery4(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Observation");
                MongoIterable<Document> iterable = collection.find(
                        and(
                                in("sensor.id", sensorIds),
                                gt("timeStamp", startTime),
                                lt("timeStamp", endTime)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("sensor.id", 1)
                                .append("payload.temperature", 1)).modifiers(new Document("$explain", true));

                getExplainResults(iterable, 4);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("Observation");
                iterable = collection.find(
                        and(
                                in("sensorId", sensorIds),
                                gt("timeStamp", startTime),
                                lt("timeStamp", endTime)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("sensorId", 1)
                                .append("payload", 1)).modifiers(new Document("$explain", true));

                getExplainResults(iterable, 4);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery5(String sensorTypeName, Date startTime, Date endTime, String payloadAttribute,
                              Object startPayloadValue, Object endPayloadValue) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Observation");
                MongoIterable<Document> iterable = collection.find(
                        and(
                                eq("sensor.type_.name", sensorTypeName),
                                gt("timeStamp", startTime),
                                lt("timeStamp", endTime),
                                gt(String.format("payload.%s", payloadAttribute), startPayloadValue),
                                lt(String.format("payload.%s", payloadAttribute), endPayloadValue)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("sensor.id", 1)
                                .append("payload", 1)).modifiers(new Document("$explain", true));

                getExplainResults(iterable, 5);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                Bson lookUp = lookup("Sensor", "sensorId", "_id", "sensors");

                Bson match = match(and(
                        eq("sensors.type_.name", sensorTypeName),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        gte(String.format("payload.%s", payloadAttribute), startPayloadValue),
                        lte(String.format("payload.%s", payloadAttribute), endPayloadValue)
                ));

                Bson project = project(
                        fields(
                                excludeId(),
                                include("sensorId", "payload", "timeStamp")
                        )
                );

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("Observation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                lookUp.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider())),
                                                match.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 5);


                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Observation");

                Bson match = match(and(
                        in("sensor.id", sensorIds),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime)));

                Bson project = project(
                        fields(
                                excludeId(),
                                include("sensor.id"),
                                computed(
                                        "date",
                                        new BsonDocument("$dateToString", new BsonDocument("format", new BsonString("%Y-%m-%d"))
                                                .append("date", new BsonString("$timeStamp")))
                                )
                        )
                );

                Bson group1 = group(new BsonDocument("sensorId", new BsonString("$sensor.id")).append("date", new BsonString("$date")),
                        sum("count", 1));
                Bson group2 = group(new BsonDocument("sensorId", new BsonString("$_id.sensorId")), avg("averagePerDay", "$count"));

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("Observation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                match.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                group1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                group2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 6);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                match = match(and(
                        in("sensorId", sensorIds),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime)));

                project = project(
                        fields(
                                excludeId(),
                                include("sensorId"),
                                computed(
                                        "date",
                                        new BsonDocument("$dateToString", new BsonDocument("format", new BsonString("%Y-%m-%d"))
                                                .append("date", new BsonString("$timeStamp")))
                                )
                        )
                );

                group1 = group(new BsonDocument("sensorId", new BsonString("$sensorId")).append("date", new BsonString("$date")),
                        sum("count", 1));
                group2 = group(new BsonDocument("sensorId", new BsonString("$_id.sensorId")), avg("averagePerDay", "$count"));

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("Observation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                match.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                group1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                group2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 6);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery7(String startLocation, String endLocation, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                Bson match1 = match(and(
                        eq("type_.name", "presence"),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("payload.location", startLocation)
                ));

                Bson lookUp = lookup("SemanticObservation", "semanticEntity.id",
                        "semanticEntity.id", "semantics");

                Bson unwind = unwind("$semantics");

                Bson project1 = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semantics"),
                                include("semanticEntity"),
                                include("payload"),
                                computed("timeCheck",
                                        new BsonDocument("$gt", new BsonArray(Arrays.asList(new BsonString("$semantics.timeStamp"), new BsonString("$timeStamp")))))
                        )
                );

                Bson match2 = match(and(
                        eq("semantics.type_.name", "presence"),
                        lt("semantics.timeStamp", endTime),
                        eq("semantics.payload.location", endLocation),
                        eq("timeCheck", true)
                ));

                Bson project2 = project(
                        fields(
                                include("semanticEntity.name")
                        )
                );

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("SemanticObservation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                match1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                lookUp.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                unwind.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                match2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 7);

                Instant end = Instant.now();
                return Duration.between(start, end);

            case 2:
                start = Instant.now();

                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();

                Bson lookUp1 = lookup("SemanticObservationType", "typeId", "_id", "type_");

                match1 = match(and(
                        eq("type_.name", "presence"),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("payload.location", startLocation)
                ));

                Bson lookUp2 = lookup("SemanticObservation", "semanticEntityId",
                        "semanticEntityId", "semantics");

                unwind = unwind("$semantics");

                project1 = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semantics"),
                                include("semanticEntityId"),
                                include("payload"),
                                computed("timeCheck",
                                        new BsonDocument("$gt", new BsonArray(Arrays.asList(new BsonString("$semantics.timeStamp"), new BsonString("$timeStamp"))))),
                                computed("typeCheck",
                                        new BsonDocument("$eq", new BsonArray(Arrays.asList(new BsonString("$semantics.typeId"), new BsonString("$typeId")))))
                        )
                );

                match2 = match(and(
                        lt("semantics.timeStamp", endTime),
                        eq("semantics.payload.location", endLocation),
                        eq("timeCheck", true),
                        eq("typeCheck", true)
                ));

                Bson lookUp3 = lookup("User", "semanticEntityId",
                        "_id", "semanticEntity");

                Bson unwind2 = unwind("$semanticEntity");

                project2 = project(
                        fields(
                                include("semanticEntity.name")
                        )
                );

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("SemanticObservation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                lookUp1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                match1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                lookUp2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                unwind.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                match2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                lookUp3.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                unwind2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 7);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery8(String userId, Date date) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection collection = database.getCollection("SemanticObservation");
                Date startTime = date;
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                Date endTime = cal.getTime();

                Bson match1 = match(and(
                        eq("type_.name", "presence"),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("semanticEntity.id", userId)
                ));

                Bson lookUp = lookup("SemanticObservation", "semanticEntity.id",
                        "semanticEntity.id", "semantics");

                Bson unwind = unwind("$semantics");

                Bson project1 = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semantics"),
                                include("semanticEntity"),
                                include("payload"),
                                computed("timeCheck",
                                        new BsonDocument("$eq", new BsonArray(Arrays.asList(new BsonString("$semantics.timeStamp"), new BsonString("$timeStamp"))))),
                                computed("placeCheck",
                                        new BsonDocument("$eq", new BsonArray(Arrays.asList(new BsonString("$semantics.payload.location"), new BsonString("$payload.location")))))
                        )
                );

                Bson match2 = match(and(
                        eq("semantics.type_.name", "presence"),
                        lt("semantics.timeStamp", endTime),
                        eq("timeCheck", true),
                        eq("timeCheck", true)
                ));

                Bson project2 = project(
                        fields(
                                include("semantics.semanticEntity.name"),
                                include("payload.location")
                        )
                );

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("SemanticObservation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                match1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                lookUp.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                unwind.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                match2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 8);

                Instant end = Instant.now();
                return Duration.between(start, end);

            case 2:
                start = Instant.now();

                startTime = date;
                cal = Calendar.getInstance();
                cal.setTime(date);
                cal.add(Calendar.DATE, 1);
                endTime = cal.getTime();

                Bson lookUp1 = lookup("SemanticObservationType", "typeId", "_id", "type_");

                match1 = match(and(
                        eq("type_.name", "presence"),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("semanticEntityId", userId)
                ));

                Bson lookUp2 = lookup("SemanticObservation", "payload.location",
                        "payload.location", "semantics");

                unwind = unwind("$semantics");

                project1 = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semantics"),
                                include("semanticEntityId"),
                                include("payload"),
                                computed("timeCheck",
                                        new BsonDocument("$eq", new BsonArray(Arrays.asList(new BsonString("$semantics.timeStamp"), new BsonString("$timeStamp"))))),
                                computed("typeCheck",
                                        new BsonDocument("$eq", new BsonArray(Arrays.asList(new BsonString("$semantics.typeId"), new BsonString("$typeId")))))
                        )
                );

                match2 = match(and(
                        ne("semantics.semanticEntityId", userId),
                        lt("semantics.timeStamp", endTime),
                        eq("timeCheck", true),
                        eq("typeCheck", true)
                ));

                Bson lookUp3 = lookup("User", "semantics.semanticEntityId",
                        "_id", "semanticEntity");

                Bson unwind2 = unwind("$semanticEntity");

                project2 = project(
                        fields(
                                include("semanticEntity.name"),
                                include("payload.location")
                        )
                );

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("SemanticObservation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                lookUp1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                match1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                lookUp2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                unwind.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                match2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                lookUp3.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                unwind2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 8);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery9(String userId, String infraTypeName) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("SemanticObservation");

                Bson lookUp1 = lookup("Infrastructure", "payload.location", "_id", "infra");

                Bson unwind = unwind("$infra");

                Bson match = match(and(
                        eq("infra.type_.name", infraTypeName),
                        eq("semanticEntity.id", userId)));

                Bson project = project(
                        fields(
                                excludeId(),
                                computed(
                                        "date",
                                        new BsonDocument("$dateToString", new BsonDocument("format", new BsonString("%Y-%m-%d"))
                                                .append("date", new BsonString("$timeStamp")))
                                )
                        )
                );

                Bson group1 = group(new BsonDocument("date", new BsonString("$date")), sum("count", new BsonInt32(1)));
                Bson group2 = group(null, avg("averageMinsPerDay", "$count"));

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("SemanticObservation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                lookUp1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                unwind.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                match.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                group1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                group2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 9);

                Instant end = Instant.now();
                return Duration.between(start, end);

            case 2:
                start = Instant.now();

                lookUp1 = lookup("Infrastructure", "payload.location", "_id", "infra");

                unwind = unwind("$infra");

                match = match(and(
                        eq("infra.type_.name", infraTypeName),
                        eq("semanticEntityId", userId)));

                project = project(
                        fields(
                                excludeId(),
                                computed(
                                        "date",
                                        new BsonDocument("$dateToString", new BsonDocument("format", new BsonString("%Y-%m-%d"))
                                                .append("date", new BsonString("$timeStamp")))
                                )
                        )
                );

                group1 = group(new BsonDocument("date", new BsonString("$date")), sum("count", new BsonInt32(10)));

                group2 = group(null, avg("averageMinsPerDay", "$count"));

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("SemanticObservation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                lookUp1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                unwind.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                match.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                group1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                group2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 9);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration explainQuery10(Date startTime, Date endTime) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("SemanticObservation");

                Bson match = match(and(
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("type_.name", "occupancy")
                ));

                Bson sort = sort(new BsonDocument("semanticEntity.id", new BsonInt32(1)).append("timeStamp", new BsonInt32(1)));

                Bson project = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("semanticEntity.name"),
                                include("payload.occupancy")
                        )
                );

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("SemanticObservation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                match.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                sort.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 10);

                Instant end = Instant.now();
                return Duration.between(start, end);
            case 2:
                start = Instant.now();

                collection = database.getCollection("SemanticObservation");

                Bson lookUp1 = lookup("SemanticObservationType", "typeId", "_id", "type_");

                match = match(and(
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        eq("type_.name", "occupancy")
                ));



                sort = sort(new BsonDocument("semanticEntityId", new BsonInt32(1)).append("timeStamp", new BsonInt32(1)));

                Bson lookUp2 = lookup("Infrastructure", "semanticEntityId", "_id", "infra");

                Bson unwind = unwind("$infra");

                project = project(
                        fields(
                                excludeId(),
                                include("timeStamp"),
                                include("infra.name"),
                                include("payload.occupancy")
                        )
                );

                writeDocumentToFile(database.runCommand(
                        new BsonDocument("aggregate", new BsonString("SemanticObservation"))
                                .append("pipeline", new BsonArray(
                                        Arrays.asList(
                                                lookUp1.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                match.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                sort.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                lookUp2.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                unwind.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())),
                                                project.toBsonDocument(BsonDocument.class, CodecRegistries.fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider())))))
                                .append("explain", new BsonBoolean(true))), 10);

                end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

}
