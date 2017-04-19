# -*- encoding: utf8 -*-
import MySQLdb
import datetime
import dateutil
import sys

if len(sys.argv) < 6:
	print (' Error!!! Not enough input parameter, program  will be exit no action!!!' )
        print ('Usage: ' + sys.argv[0] + ' hotel_list' + ' fc_start' + ' fc_end' + ' fc_days' + ' fc_type' + ' ' + 'db_info')
	sys.exit()
 
hotel_id = sys.argv[1].split(",")
start_fc_dt = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d") 
end_fc_dt   = datetime.datetime.strptime(sys.argv[3], "%Y-%m-%d")
fc_days = int(sys.argv[4])
fc_type = sys.argv[5]
db_info = sys.argv[6]

db_host = db_info.split(",")[0]
db_name = db_info.split(",")[1]
db_port = db_info.split(",")[2]
db_user = db_info.split(",")[3]
db_psswd = db_info.split(",")[4]


if fc_type == 'SEG':
    para_typ = '0,1'
elif fc_type == 'ROOM':
    para_typ = '2'
elif fc_type == 'ALL':
    para_typ = '0,1,2'
else:
    para_typ =''


print ( sys.argv[0] + ' ' + sys.argv[1]  +' '+ sys.argv[2] + ' ' + sys.argv[3] + ' ' + sys.argv[4] + ' ' + sys.argv[5] + ' ' + sys.argv[6]  )


rms_slave = {
    'host': db_host,
    'port': int(db_port),
    'user': db_user,
    'passwd': db_psswd,
    'db': db_name,
    'charset': 'utf8'
}
print ( rms_slave )
#sys.exit()
conn = MySQLdb.connect(**rms_slave)
cur = conn.cursor()
#hotel_id = ['234361']
#hotel_id = ['221065','102026']
#start_fc_dt = datetime.datetime.strptime("2016-05-01", "%Y-%m-%d")
#end_fc_dt = datetime.datetime.strptime("2016-05-25", "%Y-%m-%d")
total_day = (end_fc_dt - start_fc_dt).days
fc_dt_list = []
date_dict = {}


for hotel in hotel_id:
    if hotel not in date_dict:
        date_dict[hotel] = {}
    for i in range(total_day + 1):
        fc_dt = (start_fc_dt + datetime.timedelta(i))

        live_dt_list = []

        for j in range(fc_days + 1):
            live_dt = (fc_dt + datetime.timedelta(j)).date()
            live_dt_list.append(str(live_dt))

        fc_dt = str(fc_dt.date())
        if fc_dt not in date_dict[hotel]:
            date_dict[hotel][fc_dt] = []
        date_dict[hotel][fc_dt] = live_dt_list

num = 0
sql = "delete from rms_forecast_detail_loaddata where htl_cd = '%s' and live_dt = '%s' and fc_dt='%s' and para_typ in (%s)"
sql_for_print ="delete from rms_forecast_detail_loaddata where htl_cd = '%s' and fc_dt='%s' and para_typ in (%s)"

for hotel in date_dict:
    for fc_dt in date_dict[hotel]:
        print sql_for_print % (hotel, fc_dt, para_typ), ";" 
        for live_dt in date_dict[hotel][fc_dt]:
            cur.execute(sql % (hotel, live_dt, fc_dt, para_typ))
            num = num + 1
            #print sql % (hotel, live_dt, fc_dt, para_typ), ";" 
	    if num == 5000:
            	conn.commit()
		num = 0
        conn.commit()
