#!/bin/bash
#add by hadoop at 2016-01-21 17:32:21 

ROOT_DIR=/hadoop/1/bi_rms_v2/models/pred

sh -x /usr/local/spark-1.5.1/bin/spark-submit --jars $(echo ${ROOT_DIR}/assemblies/*.jar | tr ' ' ',') --class cn.jw.rms.data.framework.core.Entrance --master yarn-client --executor-memory 4G --driver-memory 4G --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ${ROOT_DIR}/framework-core-assembly-1.0.jar ${ROOT_DIR}/conf/application.conf