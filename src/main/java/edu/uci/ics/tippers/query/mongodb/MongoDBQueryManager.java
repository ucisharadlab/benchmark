package edu.uci.ics.tippers.query.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.common.constants.Constants;
import edu.uci.ics.tippers.connection.mongodb.DBManager;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.query.BaseQueryManager;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.*;


public class MongoDBQueryManager extends BaseQueryManager{

    private MongoDatabase database;

    public MongoDBQueryManager(int mapping, String queriesDir, boolean writeOutput, long timeout) {
        super(mapping, queriesDir, writeOutput, timeout);
        database = DBManager.getInstance().getDatabase();
    }

    @Override
    public Database getDatabase() {
        return Database.MONGODB;
    }

    @Override
    public void cleanUp() {

    }

    @Override
    public Duration runQuery1(String sensorId) throws BenchmarkException {
        switch (mapping) {
            case 1:
                Instant start = Instant.now();

                MongoCollection<Document> collection = database.getCollection("Sensor");
                MongoIterable<Document> iterable = collection.find(eq("id", sensorId))
                        .projection(new Document("name", 1).append("_id", 0));

                iterable.forEach((Consumer<? super Document>) e -> {
                    if (writeOutput) {
                        // TODO: Write To File
                        System.out.println(e.toJson());
                    }
                });
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
                        eq("sensorType.name", sensorTypeName),
                        in("coverage.entitiesCovered.id", locationIds)))
                        .projection(new Document("name", 1)
                                .append("_id", 0)
                                .append("id", 1));

                iterable.forEach((Consumer<? super Document>) e -> {
                    if (writeOutput) {
                        // TODO: Write To File
                        System.out.println(e.toJson());
                    }
                });
                Instant end = Instant.now();
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
                                .append("payload.temperature", 1));

                iterable.forEach((Consumer<? super Document>) e -> {
                    if (writeOutput) {
                        // TODO: Write To File
                        System.out.println(e.toJson());
                    }
                });
                Instant end = Instant.now();
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

                iterable.forEach((Consumer<? super Document>) e -> {
                    if (writeOutput) {
                        // TODO: Write To File
                        System.out.println(e.toJson());
                    }
                });
                Instant end = Instant.now();
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
                                eq("sensor.sensorType.name", sensorTypeName),
                                gt("timeStamp", startTime),
                                lt("timeStamp", endTime),
                                gt(String.format("payload.%s", payloadAttribute), startPayloadValue),
                                lt(String.format("payload.%s", payloadAttribute), endPayloadValue)))
                        .projection(new Document("timeStamp", 1)
                                .append("_id", 0)
                                .append("sensor.id", 1)
                                .append("payload.temperature", 1));

                iterable.forEach((Consumer<? super Document>) e -> {
                    if (writeOutput) {
                        // TODO: Write To File
                        System.out.println(e.toJson());
                    }
                });
                Instant end = Instant.now();
                return Duration.between(start, end);
            default:
                throw new BenchmarkException("No Such Mapping");
        }
    }

    @Override
    public Duration runQuery6(List<String> sensorIds, Date startTime, Date endTime) throws BenchmarkException {
        return Constants.MAX_DURATION;
    }
}
