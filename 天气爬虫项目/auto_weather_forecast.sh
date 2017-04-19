#!/usr/bin/sh

python /data/zhangwencheng/work/weather/bin/weather_forecast.py
d=`date -d  "-1 seconds" +%Y%m%d`
/data/mysql/bin/mysql -udevelop -pdevelop --database rms  -e "LOAD DATA local INFILE '/data/zhangwencheng/work/weather/data/weather_by_day/weather_data/weather_forecast_${d}.txt' INTO TABLE weather_forecast FIELDS TERMINATED BY '#' LINES TERMINATED BY '\n';"
#mysql -udevelop -pdevelop --database rms  -e "LOAD DATA local INFILE '/data/zhangwencheng/work/weather/data/weather_history.txt' INTO TABLE weather_history FIELDS TERMINATED BY '#' LINES TERMINATED BY '\n';"

