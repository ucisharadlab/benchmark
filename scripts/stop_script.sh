# MongoDB
echo "Stopping MongoDB"
mongod --shutdown  -dbpath /home/benchmark/opt/mongodb/data/

# GridDB
echo "Stopping GridDB"
export PATH=$PATH:/home/benchmark/opt/griddb/bin
export GS_HOME=/home/benchmark/opt/griddb
export GS_LOG=/home/benchmark/opt/griddb/log
export no_proxy=127.0.0.1
gs_stopcluster -u admin/admin
sleep 5
gs_stopnode -u admin/admin

# AsterixDB
echo "Stopping AsterixDB"
/home/benchmark/opt/asterixdb/opt/local/bin/stop-sample-cluster.sh

# PostgreSQL
echo "Stopping Postgres"
service postgresql stop

# Cassandra
echo "Stopping Cassandra"
service cassandra stop

# CrateDB
echo "Stopping CrateDB"
service crate stop
