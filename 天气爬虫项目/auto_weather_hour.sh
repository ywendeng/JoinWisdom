#!/usr/bin/sh

d=`date -d  "-1 seconds" +%Y%m%d%H`
mkdir /data/zhangwencheng/work/weather/data/weather_by_hour/weather_data/$d
python /data/zhangwencheng/work/weather/bin/weather_hour.py
cat /data/zhangwencheng/work/weather/data/weather_by_hour/weather_data/$d/*  > /data/zhangwencheng/work/weather/data/weather_by_hour/weather_data/weather_forecast_3hours_${d}.txt
rm -r  /data/zhangwencheng/work/weather/data/weather_by_hour/weather_data/$d
/data/mysql/bin/mysql -udevelop -pdevelop --database rms  -e "LOAD DATA local INFILE '/data/zhangwencheng/work/weather/data/weather_by_hour/weather_data/weather_forecast_3hours_${d}.txt' INTO TABLE weather_hour FIELDS TERMINATED BY '#' LINES TERMINATED BY '\n';"
