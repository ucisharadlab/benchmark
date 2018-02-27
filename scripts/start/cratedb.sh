echo  "Starting CrateDB ... \n\n"
CRATE_HEAP_SIZE=2g
echo benchmark | sudo -S service crate start
sleep 60
