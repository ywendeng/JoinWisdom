#!/bin/bash
#add by hadoop at 2016-01-21 17:32:21 

ROOT_DIR=V_ROOT_DIR
APP_CONF=V_APP_CONF

sh -x /usr/local/spark-1.5.1/bin/spark-submit --jars $(echo ${ROOT_DIR}/assemblies/*.jar | tr ' ' ',') --class cn.jw.rms.data.framework.core.Entrance --master yarn-client --executor-memory 8G --driver-memory 16G --num-executors 10 --conf spark.default.parallelism=20 --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ${ROOT_DIR}/framework-core-assembly-1.3.jar ${ROOT_DIR}/conf/${APP_CONF}


