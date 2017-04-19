# -*- encoding: utf8 -*-
from datetime import datetime
da=datetime.now().strftime('%Y%m%d%H')
dat=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# 超时文件路径
failedpath = "/data/ywendeng/work/weather/data/weather_by_hour/failed_data/failed_forecast_3hours_"+da+".txt"
#分区文件路径
partition_file="/data/ywendeng/work/weather/data/weather_by_hour/weather_data/"+da+"/"
#城市信息
citypath="/data/ywendeng/work/weather/data/city_id_coord.txt"
#最大线程数
max_thread=100
# 超时时间(秒)
deftimeout = 10
# 睡眠时间(秒)
sleeptime = 0.5
# 超时重试次数
retrytimes = 5
cityfromfile=True
