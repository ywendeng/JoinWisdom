# -*- encoding: utf8 -*-

from datetime import datetime 
startdate=datetime.strptime(str(int(datetime.now().strftime("%Y%m"))-1),"%Y%m").strftime('%Y-%m-%d')
enddate=datetime.now().strftime("%Y-%m-%d")
dt=datetime.now().strftime("%Y%m%d")
# weather文件路径
weatherpath = "/data/ywendeng/work/weather/data/weather_history/weather_history_"+dt+".txt"
# 超时文件路径
failedpath = "/data/ywendeng/work/weather/data/weather_history/failed_history.txt"
#城市信息
citypath="/data/ywendeng/work/weather/data/city.txt"
#istartdate = "2012-01-01"
# 结束日期
#enddate = "2016-08-01"
# 超时时间(秒)
deftimeout = 10
# 睡眠时间(秒)
sleeptime = 0.5
# 超时重试次数
retrytimes = 5
