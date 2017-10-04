echo  "Starting MongoDB ... \n\n"
export PATH=$PATH:/home/benchmark/opt/mongodb/bin
mongod --fork --logpath /tmp/mongodb.log -dbpath /home/benchmark/opt/mongodb/data/
