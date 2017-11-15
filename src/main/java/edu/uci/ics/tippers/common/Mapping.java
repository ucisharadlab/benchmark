package edu.uci.ics.tippers.common;

public enum Mapping {

    MONGO_SUBOBJECT(1, Database.MONGODB, "MongoDB Sub Object Model Mapping"),
    MONGO_FLAT(2, Database.MONGODB, "MongoDB Flat Model"),

    ASTERIXDB_SUBOBJECT(1, Database.ASTERIXDB, "AsterixDB Sub Object Model"),
    ASTERIXDB_FLAT(2, Database.ASTERIXDB, "AsterixDB Flat Model"),

    PGSQL_SINGLETABLE(1, Database.POSTGRESQL, "PostgreSQL Single Table For All Observations"),
    PGSQL_MULTITABLE(2, Database.POSTGRESQL, "PostgreSQL Table Per Sensor Type"),

    CRATEDB_SINGLETABLE(1, Database.CRATEDB, "CrateDB Single Table For All Observations"),
    CRATEDB_MULTITABLE(2, Database.CRATEDB, "CrateDB Table Per Sensor Type"),

    GRIDDB_TIMESERIES(1, Database.GRIDDB, "GridDB Timeseries Per Sensor Model"),
    GRIDDB_COLLECTION(2, Database.GRIDDB, "GridDB Collection Per Sensor Type Model"),;

    private final int mapping;
    private final Database database;
    private final String description;

    Mapping(int mapping, Database db, String description) {
        this.mapping = mapping;
        this.database = db;
        this.description = description;
    }

    public int getMapping() {
        return mapping;
    }

    public Database getDatabase() {
        return database;
    }

    public String getDescription() {
        return description;
    }
}
