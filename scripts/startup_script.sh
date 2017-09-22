# MongoDB
echo  "Starting MongoDB ... \n\n"
export PATH=$PATH:/home/benchmark/opt/mongodb/bin
mongod --fork --logpath /tmp/mongodb.log -dbpath /home/benchmark/opt/mongodb/data/

# GridDB
echo  "Starting GridDB ... \n\n"
export PATH=$PATH:/home/benchmark/opt/griddb/bin
export GS_HOME=/home/benchmark/opt/griddb
export GS_LOG=/home/benchmark/opt/griddb/log
export no_proxy=127.0.0.1
gs_startnode
sleep 5
gs_joincluster -c cluster1 -u admin/admin

# AsterixDB
echo  "Starting AsterixDB ... \n\n"
/home/benchmark/opt/asterixdb/opt/local/bin/start-sample-cluster.sh

# PostgreSQL
echo  "Starting Postgres ... \n\n"
service postgresql start

# Cassandra
echo  "Starting Cassandra ... \n\n"
service cassandra start

# CrateDB
echo  "Starting CrateDB ... \n\n"
service crate start