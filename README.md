# IoT Database Benchmark v0.1

At present the Benchmark compares the following databases:

MongoDB CE (3.4.9)
GridDB SE (3.0.1)
AsterixDB (0.9.2)
PostgreSQL (9.6.5)
Cassandra (3.11)
Crate DB (2.1.6)

For each database we use different mappings to store TIPPERS data. For each such mapping we add code for schema creation,
data upload and query translation.

Benchmark can be configured to compare a certain set of databases and mappings and other features such as report generation,
through a configuration file `benchmark.ini` situated at `<$basedir/src/main/resources/`

<h3>Example Configuration file</h3>

There are some third party jars that are required to be added to maven repository.

<h4>Third Pary GridDB jar</h4>

```
mvn install:install-file -Dfile=<$basedir/thirdparty/gridstore.jar> -DgroupId=griddb -DartifactId=org.griddb -Dversion=3.0 -Dpackaging=jar
```

<h3>Compilation</h3>

```
mvn clean install 
```

<h3>Execution</h3>

```
mvn exec:java 
```



