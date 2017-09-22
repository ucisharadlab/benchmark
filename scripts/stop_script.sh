# MongoDB
echo  "Stopping MongoDB ...\n\n"
mongod --shutdown  -dbpath /home/benchmark/opt/mongodb/data/

# GridDB
echo  "Stopping GridDB ...\n\n"
export PATH=$PATH:/home/benchmark/opt/griddb/bin
export GS_HOME=/home/benchmark/opt/griddb
export GS_LOG=/home/benchmark/opt/griddb/log
export no_proxy=127.0.0.1
gs_stopcluster -u admin/admin
sleep 5
gs_stopnode -u admin/admin

# AsterixDB
echo  "Stopping AsterixDB ... \n\n"
/home/benchmark/opt/asterixdb/opt/local/bin/stop-sample-cluster.sh

# PostgreSQL
echo  "Stopping Postgres ... \n\n"
service postgresql stop

# Cassandra
echo  "Stopping Cassandra ... \n\n"
service cassandra stop

# CrateDB
echo  "Stopping CrateDB ... \n\n"
service crate stop
