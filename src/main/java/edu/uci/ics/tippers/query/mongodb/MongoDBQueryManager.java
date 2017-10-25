package edu.uci.ics.tippers.query.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.mongodb.DBManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import edu.uci.ics.tippers.writer.RowWriter;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Projections.*;
import static edu.uci.ics.tippers.common.util.Helper.getFileFromQuery;


public class MongoDBQueryManager extends BaseQueryManager{

    private MongoDatabase database;

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

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
            case 2:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Sensor");
                MongoIterable<Document> iterable = collection.find(eq("id", sensorId))
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
                                .append("_id", 0)
                                .append("id", 1));

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
                                .append("_id", 0)
                                .append("id", 1));

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

                Bson lookUp = lookup("Sensor", "sensorId", "id", "sensors");

                Bson match = match(and(
                        eq("sensors.type_.name", sensorTypeName),
                        gt("timeStamp", startTime),
                        lt("timeStamp", endTime),
                        gt(String.format("payload.%s", payloadAttribute), startPayloadValue),
                        lt(String.format("payload.%s", payloadAttribute), endPayloadValue)
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

                Bson group1 = group(new Document("date", "$date").append("sensorId", "$sensor.id"),
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

                group1 = group(new Document("date", "$date").append("sensorId", "$sensorId"),
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

    public Duration runQuery7(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
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

                Bson group1 = group(new Document("date", "$date").append("sensorId", "$sensor.id"),
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

                group1 = group(new Document("date", "$date").append("sensorId", "$sensorId"),
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

    public Duration runQuery8(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
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

                Bson group1 = group(new Document("date", "$date").append("sensorId", "$sensor.id"),
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

                group1 = group(new Document("date", "$date").append("sensorId", "$sensorId"),
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

    public Duration runQuery9(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
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

                Bson group1 = group(new Document("date", "$date").append("sensorId", "$sensor.id"),
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

                group1 = group(new Document("date", "$date").append("sensorId", "$sensorId"),
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

    public Duration runQuery10(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
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

                Bson group1 = group(new Document("date", "$date").append("sensorId", "$sensor.id"),
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

                group1 = group(new Document("date", "$date").append("sensorId", "$sensorId"),
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
}
