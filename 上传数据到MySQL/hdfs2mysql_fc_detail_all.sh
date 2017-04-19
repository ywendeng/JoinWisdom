#!/bin/bash
#read -p "Please enter partiton:" PARTITION;
#read -p "Please enter htl_list:" HTL_LIST;
#read -p "Please enter fc_start:" FC_START;
#read -p "Please enter fc_end:" FC_END;
#read -p "Please enter fc_days:" FC_DAYS
export PATH="/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/hbase/bin:/usr/local/hive/bin:/usr/local/hive/hcatalog/bin:/usr/local/mysql/bin:/usr/local/oozie/bin:/usr/local/sqoop/bin:/usr/local/scala/bin:/usr/local/spark/bin:/usr/local/bin:/usr/local/jdk/bin:/usr/local/maven/bin:/usr/local/ant/bin:/usr/local/protobuf/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/home/bidev/bin"
source /home/bwuat/even/bin/activate
source /data/uat/bw/rms/models/newhotel/hdfs2mysql/config/mysql_db.config UAT 

if [ $# -ne 4 ] ;then
   echo "Usage: ./hdfs2mysql_fc_detail_custom.sh PARTITION HTL_LIST FC_START FC_END"
    echo "Example: ./hdfs2mysql_fc_detail_daily.sh daily_20160905 \"'DJSW000001','221065'\" 2016-08-04 2016-08-31 "
    exit -1
fi
PARTITION=$1
echo "$2" | grep '"'
if [ $? -eq 0 ]; then
HTL_LIST=`echo $2 | cut -d '"' -f 2 `
else
HTL_LIST=$2
fi
FC_START=$3
FC_END=$4
YESTER_DAY=`date -d "yesterday" +%Y-%m-%d`
TODAY_DAY=`date -d "-1 seconds" +%Y-%m-%d`


echo -e "You have enter partiton:$PARTITION , htl_list:${HTL_LIST}, fc_start:$FC_START , fc_end:$FC_END "
#MYSQL_HOST=106.75.28.62
#MYSQL_USER=loaddata
#OS_USER=loaddata
#MYSQL_PASSWD=loaddataloaddata
#MYSQL_HOME=/data/server/bin/mysql-5.7.13/bin
#MYSQL_PORT=6869
#MYSQL_LOGIN="$MYSQL_HOME/mysql -u$MYSQL_USER -p$MYSQL_PASSWD -h$MYSQL_HOST -P$MYSQL_PORT"
#MYSQL_DB=rms
SPLIT_NUM=10000
FC_DAYS=396
FC_TYPE="ALL"
DB_INFO="$MYSQL_HOST,$MYSQL_DB,$MYSQL_PORT,$MYSQL_USER,$MYSQL_PASSWD"
REDO_TIMES=5
SLEEP_SECONDS=600

echo "[MYSQL_PORT:$MYSQL_DB] [MYSQL_DB:$MYSQL_PORT] [MYSQL_LOGIN:$MYSQL_LOGIN]"

if [ "$HTL_LIST" != "all" ] ;then
FILTER_HTL="and htl_cd in ($HTL_LIST)"
else
FILTER_HTL=""
fi

HIVE_TABLE=rms_forecast_detail
MYSQL_TABLE=rms_forecast_detail
HDFS_ROOT="hdfs://ns1/uat/bw/rms/"
LOCAL_ROOT="/data/uat/bw/rms/models/newhotel/hdfs2mysql/"
REMOTE_ROOT="/data/loaddata/rms/"
AWK_TPL_PATH=$LOCAL_ROOT/load.tpl

#hadoop fs -test -e $HDFS_ROOT/rms_forecast_detail/dt=${PARTITION}/
hadoop fs -test -e $HDFS_ROOT/newest_rms_forecast_detail/dt=${PARTITION}/
#echo"$?">$LOCAL_ROOT/logs.txt
#exit 1
if [ $? -ne 0 ]; then
       echo "$0 running fail with $HDFS_ROOT/newest_rms_forecast_detail/dt=${PARTITION}/ not exist!!!"
       echo "please check the upsteam program is error!!!"
       exit 1
fi

echo $MYSQL_DEL_SQL
echo "--------[`date`]--  Implement  [ Start  Delete table data ]"
/home/bwuat/even/bin/python2.7 $LOCAL_ROOT/delete_fc_detail_by_fcdt.py $HTL_LIST $FC_START $FC_END $FC_DAYS $FC_TYPE $DB_INFO 

if [ $? -ne 0 ]; then
   for((i=1;i<= ${REDO_TIMES};i++))
   do
        echo $i
        sleep $SLEEP_SECONDS
        /home/bwuat/even/bin/python2.7 $LOCAL_ROOT/delete_fc_detail_by_fcdt.py $HTL_LIST $FC_START $FC_END $FC_DAYS $FC_TYPE $DB_INFO
        if [ $? -ne 0 ]; then
            continue 
        else
            OK_I=$i
            break
        fi
        
   done
   echo "${OK_I} , ${REDO_TIMES}" 
   if [ ${OK_I} -gt  ${REDO_TIMES} ]; then
   	echo "delete fc_detail failed the program will be exit !"
   	exit 1 
   else
        echo "delete fc_detail has retry $i times, and sucess in last time"
   fi
fi

echo "--------[`date`]--  Implement  [ Finish Delete table data ]"
#hdfs://ns1/uat/bw/rms/middata/history/split_pred_wsf_sum/dt=daily_178140_20160926_1340_online_178140
#insert detail
#seg 
#rm -r $LOCAL_ROOT/dt=${PARTITION}
#hadoop fs -get "$HDFS_ROOT/rms_forecast_detail/dt=${PARTITION}/" $LOCAL_ROOT
hadoop fs -cat $HDFS_ROOT/newest_rms_forecast_detail/dt=${PARTITION}/* > $LOCAL_ROOT/upload_data
#cat $LOCAL_ROOT/dt=${PARTITION}/part* > $LOCAL_ROOT/upload_data
#rm -r $LOCAL_ROOT/dt=${PARTITION}
#hadoop fs -get "$HDFS_ROOT/room/room_daily_result/dt=${PARTITION}/" $LOCAL_ROOT
#room
hadoop fs -cat $HDFS_ROOT/room/newest_room_daily_result/dt=${PARTITION}/* >> $LOCAL_ROOT/upload_data
#cat $LOCAL_ROOT/dt=${PARTITION}/part* >> $LOCAL_ROOT/upload_data
#rm -r $LOCAL_ROOT/dt=${PARTITION}
#hotel su
hadoop fs -cat $HDFS_ROOT/newest_rms_forecast_detail_sum/dt=${PARTITION}/* >> $LOCAL_ROOT/upload_data
#cat $LOCAL_ROOT/dt=${PARTITION}/part* >> $LOCAL_ROOT/upload_data
#rm -r $LOCAL_ROOT/dt=${PARTITION}



cd $LOCAL_ROOT
TAR_FILE=$HIVE_TABLE.tar.gz
if [ -f $TAR_FILE ];then
        rm -rf $TAR_FILE
fi
tar zcvf $TAR_FILE ./upload_data

echo "
        if [ -d $REMOTE_ROOT/$HIVE_TABLE ];then
                rm -rf $REMOTE_ROOT/$HIVE_TABLE
        fi
        mkdir $REMOTE_ROOT/$HIVE_TABLE

" |ssh $OS_USER@$MYSQL_HOST

scp $TAR_FILE $OS_USER@$MYSQL_HOST:$REMOTE_ROOT/$HIVE_TABLE/

echo "
        if [ -f $REMOTE_ROOT/$HIVE_TABLE/$TAR_FILE ];then
                tar zxf $REMOTE_ROOT/$HIVE_TABLE/$TAR_FILE -C $REMOTE_ROOT/$HIVE_TABLE/
                rm -rf $REMOTE_ROOT/$HIVE_TABLE/$TAR_FILE
        fi
" |ssh $OS_USER@$MYSQL_HOST

echo "--------[`date`]--  Implement  [ inster into table ]"
SPLIT_OPT="SPLIT"
echo "rm $REMOTE_ROOT/$HIVE_TABLE/split_*" | ssh $OS_USER@$MYSQL_HOST

echo "split -l $SPLIT_NUM $REMOTE_ROOT/$HIVE_TABLE/upload_data $REMOTE_ROOT/$HIVE_TABLE/split_ " | ssh $OS_USER@$MYSQL_HOST
echo "/data/loaddata/rms/loaddata_2_table.sh $MYSQL_HOME $MYSQL_USER $MYSQL_PASSWD $MYSQL_HOST $MYSQL_PORT $MYSQL_DB $HIVE_TABLE $SPLIT_OPT " | ssh $OS_USER@$MYSQL_HOST

echo "rm $REMOTE_ROOT/$HIVE_TABLE/split_*" | ssh $OS_USER@$MYSQL_HOST
echo "--------[`date`]--  Implement  [ finish  inster into table ]"

