#!/bin/bash
hadoop fs -rm -R output
hadoop fs -rm -R input
hadoop fs -mkdir input
hadoop fs -put ./$3 input
hadoop jar /opt/server/hadoop-2.5.2/share/hadoop/tools/lib/hadoop-streaming-2.5.2.jar \
-input input/$3	  \
-output output/$4 \
-mapper "python ./$1" \
-reducer "python ./$2" \
-file "./$1" \
-file "./$2"
hadoop fs -get output/$4 ./$4