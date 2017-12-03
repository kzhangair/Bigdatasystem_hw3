#!/bin/bash
hadoop fs -rm -R output
hadoop jar /opt/server/hadoop-2.5.2/share/hadoop/tools/lib/hadoop-streaming-2.5.2.jar \
-input input/PlainText.txt \
-output output \
-mapper "python ./mapper.py" \
-reducer "python ./reducer.py" \
-file "./mapper.py" \
-file "./reducer.py"