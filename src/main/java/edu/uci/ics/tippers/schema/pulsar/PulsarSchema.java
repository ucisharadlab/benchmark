package edu.uci.ics.tippers.schema.pulsar;

import edu.uci.ics.tippers.common.Database;
import edu.uci.ics.tippers.connection.pulsar.PulsarConnectionManager;
import edu.uci.ics.tippers.data.pulsar.PulsarDataUploader;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.schema.BaseSchema;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class PulsarSchema extends BaseSchema {

    private String CREATE_SCHEMA_FORMAT = "pulsar/schema/mapping%s/create.sql";
    PulsarConnectionManager connectionManager = PulsarConnectionManager.getInstance();

    public PulsarSchema(int mapping, String dataDir) {
        super(mapping, dataDir);
    }

    @Override
    public Database getDatabase() {
        return Database.PULSAR;
    }

    private String readSchema(int mapping) throws BenchmarkException {
        byte[] encoded = new byte[0];
        try {
            encoded = Files.readAllBytes(Paths.get(
                    PulsarDataUploader.class.getClassLoader().getResource(
                            String.format(CREATE_SCHEMA_FORMAT, mapping)).getPath()));
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Reading Pulsar Schema File");
        }
        return new String(encoded, Charset.defaultCharset());
    }

    @Override
    public void createSchema() throws BenchmarkException {
        connectionManager.createSchemaFile(readSchema(mapping));
    }

    @Override
    public void dropSchema() throws BenchmarkException {

    }
}
