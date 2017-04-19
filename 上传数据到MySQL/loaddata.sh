#!/bin/bash
MYSQL_HOME=$1
MYSQL_USER=$2
MYSQL_PASSWD=$3
MYSQL_HOST=$4
MYSQL_PORT=$5
MYSQL_DB=$6
cd /data/loaddata/rms//rms_forecast_detail_cust/
for split_file in `ls ./split_*`
do
awk -F '#' -f ./load.tpl ./$split_file | $MYSQL_HOME/mysql -u$MYSQL_USER -p$MYSQL_PASSWD -h$MYSQL_HOST -P$MYSQL_PORT -D$MYSQL_DB
done
