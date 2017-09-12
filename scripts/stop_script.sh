# MongoDB
mongod --shutdown

# GridDB
export PATH=$PATH:/home/benchmark/opt/griddb/bin
export GS_HOME=/home/benchmark/opt/griddb
export GS_LOG=/home/benchmark/opt/griddb/log
export no_proxy=127.0.0.1
gs_stopcluster -c cluster1 -u admin/admin
sleep 5
gs_stopnode

# AsterixDB
/home/benchmark/opt/asterixdb/opt/local/bin/stop-sample-cluster.sh

# PostgreSQL
service postgresql stop

# Cassandra
service cassandra stop

# CrateDB
service crate stop
