echo  "Starting MongoDB ... \n\n"
export PATH=$PATH:/home/benchmark/opt/mongodb/bin
mongod --fork  --config /home/benchmark/benchmark/benchmark/configuration/mongodb/mongod1.conf --logpath /tmp/mongodb.log -dbpath /home/benchmark/opt/mongodb/data/
