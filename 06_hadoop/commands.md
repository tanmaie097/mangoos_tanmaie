#### hdfs dfsadmin -report

Configured Capacity: 994662584320 (926.35 GB)
Present Capacity: 546357424128 (508.84 GB)
DFS Remaining: 546357420032 (508.83 GB)
DFS Used: 4096 (4 KB)
DFS Used%: 0.00%
Replicated Blocks:
	Under replicated blocks: 0
	Blocks with corrupt replicas: 0
	Missing blocks: 0
	Missing blocks (with replication factor 1): 0
	Low redundancy blocks with highest priority to recover: 0
	Pending deletion blocks: 0
Erasure Coded Block Groups: 
	Low redundancy block groups: 0
	Block groups with corrupt internal blocks: 0
	Missing block groups: 0
	Low redundancy blocks with highest priority to recover: 0
	Pending deletion blocks: 0

-------------------------------------------------
Live datanodes (1):

Name: 127.0.0.1:9866 (localhost)
Hostname: localhost
Decommission Status : Normal
Configured Capacity: 994662584320 (926.35 GB)
DFS Used: 4096 (4 KB)
Non DFS Used: 448305160192 (417.52 GB)
DFS Remaining: 546357420032 (508.83 GB)
DFS Used%: 0.00%
DFS Remaining%: 54.93%
Configured Cache Capacity: 0 (0 B)
Cache Used: 0 (0 B)
Cache Remaining: 0 (0 B)
Cache Used%: 100.00%
Cache Remaining%: 0.00%
Xceivers: 0
Last contact: Fri Dec 12 12:09:15 GST 2025
Last Block Report: Fri Dec 12 12:08:55 GST 2025
Num of Blocks: 0

# START HADOOP
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
jps

# STOP HADOOP
$HADOOP_HOME/sbin/stop-yarn.sh
$HADOOP_HOME/sbin/stop-dfs.sh

