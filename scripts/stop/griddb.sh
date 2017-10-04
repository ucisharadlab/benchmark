echo  "Stopping GridDB ...\n\n"
export PATH=$PATH:/home/benchmark/opt/griddb/bin
export GS_HOME=/home/benchmark/opt/griddb
export GS_LOG=/home/benchmark/opt/griddb/log
export no_proxy=127.0.0.1
gs_stopcluster -u admin/admin
sleep 5
gs_stopnode -u admin/admin
sleep 5