# IoT Database Benchmark v0.1

<h3> Resources </h3>
[Benchmark Paper - Long Version](https://github.com/ucisharadlab/benchmark/blob/master/smarBench-longVersion.pdf) 

At present the Benchmark compares the following databases:

```
MongoDB CE (3.4.9)
GridDB SE (3.0.1)
AsterixDB (0.9.2)
PostgreSQL (9.6.5)
Crate DB (2.1.6)
InfluxDB (1.5.0)
SparkSQL (2.3.0)
Couchbase (5.1.0)
```

For each database we use different mappings to store TIPPERS data. For each such mapping we add code for schema creation,
data upload and query translation.

Benchmark can be configured to compare a certain set of databases and mappings and other features such as report generation,
through a configuration file `benchmark.ini` situated at `<$basedir/src/main/resources/`. For each database to be benchmarked 
update the connection information present at `<$basedir/src/main/resources/$database/$database.properties` file. To finetune 
the benchmark further check [this file](https://github.com/ucisharadlab/benchmark/blob/master/src/main/java/edu/uci/ics/tippers/execution/Benchmark.java) and make changes accordingly.



<h3>Example Configuration file</h3>

```
[benchmark]
scale-data = false
scale-query = false
write-query-result = true
query-timeout = 500
databases = mongodb,griddb,postgresql,cratedb,asterixdb
scripts-dir = /home/benchmark/benchmark/benchmark/scripts/
data-dir = /home/benchmark/benchmark/benchmark/data/
queries-dir = /home/benchmark/benchmark/benchmark/queries/
query-result-dir = /home/benchmark/benchmark/benchmark/results/
reports-dir = /home/benchmark/benchmark/benchmark/reports/
report-format = text
```

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

<h3>Data Generation</h3>
Benchmark data can be generated for a given number of days, users and devices.
in order to generate data change parameters in one of the configuration files provided in python/config or create a new configuration file. The tool can also generate query parameters and separate insert test data. 
Requires Python 2.

<h3>Runnign Data Generation</h3>

``` 
cd python
python generate.py <path to config file>
```

<h3>Example Configuration file</h3>

```
[observation]
start_timestamp = 2017-11-08 00:00:00
days = 20
step = 500
pattern = random

[sensors]
wemo = 20
wifiap = 20
temperature = 20

[others]
users = 20
data-dir = /home/benchmark/benchmark/benchmark/src/main/resources/data/
output-dir = /home/benchmark/benchmark/benchmark/data/

```
<h3>Secure Databases</h3>

Benchmark also has support for the following cloud secure database systems

```
Jana
Pulsar
SQL Server Always Encrypted
```

In order to run bencmark on these systems checkout the `encryption` branch.
