# -*- encoding: utf8 -*-
from datetime import datetime
da=datetime.now().strftime('%Y%m%d')
# weather文件路径
weatherpath = "/data/ywendeng/work/weather/data/weather_by_day/weather_data/weather_forecast_"+da+".txt"
# 超时文件路径
failedpath = "/data/ywendeng/work/weather/data/weather_by_day/failed_data/failed_forecast_"+da+".txt"
#城市信息
citypath="/data/ywendeng/work/weather/data/city.txt"

# 超时时间(秒)
deftimeout = 10
# 睡眠时间(秒)
sleeptime = 0.5
# 超时重试次数
retrytimes = 5
cityfromfile=True
