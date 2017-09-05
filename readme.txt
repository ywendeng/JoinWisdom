框架:spark+scala
功能：主要用于实现了Revene Plus 中数据清洗的代码
说明：由于在工作期间代码是提交到公司的github上，所以在工作期间自己写的代码在工作结束之后集中提交到自己的github
程序执行脚本：
#!/bin/bash
export PATH="/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/hbase/bin:/usr/local/hive/bin:/usr/local/hive/hcatalog/bin:/usr/local/mysql/bin:/usr/local/oozie/bin:/usr/local/sqoop/bin:/usr/local/scala/bin:/usr/local/spark/bin:/usr/local/bin:/usr/local/jdk/bin:/usr/local/maven/bin:/usr/local/ant/bin:/usr/local/protobuf/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/home/bidev/bin:/home/bidev/.local/usr/bin"
NEW_FCDT=$1
TODAY_FCDT=`date +%Y-%m-%d`
NEW_FCDT=${NEW_FCDT:-$TODAY_FCDT}
ROOT_DIR=/data/p/bw/rms/models/prod/v2.2/2.2.0/sys_day
SCRIPT_DIR=/data/p/bw/rms/models/prod/v2.2/2.2.0/sys_day/insertData2MysqlScripts
FC_DT=`date -d "$NEW_FCDT -1 days" +%Y-%m-%d`

start_date=`date +%H:%M:%S`
spark-submit --jars $(find ${ROOT_DIR} -type f -name "*.jar" -not -name "lego-core*" | xargs echo | tr ' ' ',') --class cn.jw.rms.data.framework.core.Main --master yarn-client  --driver-memory 8G  --executor-memory 10G  --num-executors 30  --conf spark.default.parallelism=30 --conf spark.shuffle.consolidateFiles=true --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ${ROOT_DIR}/lego-core-assembly-2.0.jar ${ROOT_DIR}/conf/application.conf

end_date=`date +%H:%M:%S`
let date=$(date +%s -d"$end_date")-$(date +%s -d"$start_date")
echo -e "bidev\t$FC_DT\t$date" >> $ROOT_DIR/date_record.log
