echo  "Starting MongoDB ... \n\n"
export PATH=$PATH:/mnt/data/sdb/peeyushg/opt/mongodb/bin
mongod --fork  --config /mnt/data/sdb/peeyushg/benchmark/configuration/mongodb/mongod1.conf --logpath /tmp/mongodb.log -dbpath /mnt/data/sdb/peeyushg/opt/mongodb/data/
