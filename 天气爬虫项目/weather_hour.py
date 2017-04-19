# -# -*- coding: utf-8 -*-
import sys, urllib, urllib2, json ,time,socket,threading,multiprocessing
from datetime import datetime,timedelta
from setting_hour import failedpath,deftimeout,retrytimes,citypath,sleeptime,dat,da,partition_file,max_thread

socket.setdefaulttimeout(deftimeout)
class Test(threading.Thread):
    def __init__(self,thread_no,output_file,citys):
        threading.Thread.__init__(self)
        self.thread_no = thread_no
        self.output_file = output_file
        self.citys = citys[self.thread_no]
    def run(self):
        for x in self.citys:
            seg=x.strip().split('#')
            id=seg[0]
            city=seg[1]
            weather(id,city,self.output_file)
def weather(id,city,output_file):
    url = 'http://api.openweathermap.org/data/2.5/forecast?id='+id+'&appid=24d2588dfe4956797fe8ef3fb350cb0e'
    print url
    retry = 0
    retryflag = True
    f = open(output_file,'a+')
    while retryflag and retry <= retrytimes:
        weather_info=[]
        try:
            page = urllib.urlopen(url)
            html = page.read()
            retryflag = False
            info = eval(html)
            lat = info["city"]["coord"]["lat"]
            lon = info["city"]["coord"]["lon"]
            name = info["city"]["name"]
            weather_info = info["list"]
        except:
            retryflag = True
            retry = retry + 1
            print city + "  " + " 超时重试..."
            time.sleep(1)
        if weather_info != []:
            for w in weather_info:
                temp_min = str(float(w["main"]["temp_min"])-273.16)
                temp_max = str(float(w["main"]["temp_max"])-273.16)
                humidity = w["main"]["humidity"]
                weather=w["weather"][0]["main"]
                weather_discription=w["weather"][0]["description"]
                wind_speed=w["wind"]["speed"]
                wind_deg=w["wind"]["deg"]
                dt_txt = w["dt_txt"]
                dt = datetime.strftime((datetime.strptime(dt_txt,"%Y-%m-%d %H:%M:%S")+ timedelta(hours=8)),"%Y-%m-%d %H:%M:%S")
                #print str(id),str(lon),str(lat),name,temp_min,temp_max,humidity,weather,weather_discription,wind_speed,wind_deg,dt
                f.write('#'.join([str(id),str(lon),str(lat),name,temp_min,temp_max,str(humidity),weather,weather_discription,str(wind_speed),str(wind_deg),dt,dat]))
                f.write('\n')
            f.close()
            print name +'\t'+ 'successed!'
        else:
            tof = open(failedpath, "a+")
            tof.write(city + '\t' + url + '\t')
            tof.write('failed!' + "\n")
            tof.close()
    if (retry > retrytimes):
        tof = open(failedpath, "a+")
        tof.write(city + '\t' + url + "\t")
        tof.write('\t' + 'retry failed!' + "\n")
        tof.close()

def main(num):
    fc = open(citypath)
    citys_info = fc.readlines()
    #print citys_info
    citys_len=len(citys_info)
    #print citys_len
    task_len=citys_len/num
    #print task_len
    citys=[]
    for i in range(num-1):
        citys.append(citys_info[i*task_len:(i+1)*task_len])
    citys.append(citys_info[task_len*(num-1):])
    #print len(citys)
    #print citys
    fc.close()
    threads = []
    for x in range(num):
        output_file = partition_file + str(x)
        test=Test(x, output_file , citys)
        threads.append(test)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    #pool = multiprocessing.Pool(processes=4)
if __name__=="__main__" :
    main(max_thread)
