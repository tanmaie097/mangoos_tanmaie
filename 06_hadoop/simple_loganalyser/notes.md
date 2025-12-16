## 🌟 Mini Project: Log Analyzer on Hadoop

✔ Upload a dataset into HDFS
✔ Explore how NameNode + DataNode store the file
✔ Write MapReduce program in Python
✔ Run it on YARN
✔ Visualize results in VS Code
✔ Verify block storage, replication, and cluster behavior 

code to yarn:

(base) tanmaie@MacBook-Pro-tanmaie ~ % hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /Users/tanmaie/Desktop/magnoos/exercises/06_hadoop/simple_loganalyser/src/map.py,/Users/tanmaie/Desktop/magnoos/exercises/06_hadoop/simple_loganalyser/src/reduce.py \
  -mapper "python3 map.py" \
  -reducer "python3 reduce.py" \
  -input /user/tanmaie/logs/logs.txt \
  -output /user/tanmaie/logs/output

code to send the results back to my local system:

  hdfs dfs -cat /user/tanmaie/logs/output/part-00000 \
  > ~/Desktop/magnoos/exercises/06_hadoop/simple_loganalyser/output/results.txt

hdfs dfs -copyToLocal, -copyFromLocal 