# -*- encoding:utf8 -*-
import re,urllib,datetime,time,socket
from setting_history import weatherpath,failedpath,startdate,enddate,deftimeout,sleeptime,retrytimes,citypath

# 设置超时时间为10秒
socket.setdefaulttimeout(deftimeout)

def weather(cnpro,cncity,dis,cndis,date):
    url = "http://lishi.tianqi.com/"+dis+"/"+date+".html"
    print url
    retry = 0
    retryflag = True
    while retryflag and retry <= retrytimes:
        html = ""
        try:
            page = urllib.urlopen(url)
            html = page.read().decode('gbk')
            retryflag = False
        except:
            retryflag = True
            retry = retry + 1
            print cndis + "  " + date + " 超时重试..."
            time.sleep(1)
        pt = re.compile('<ul class="t1">(.*?)</div>',re.S)
        pa = re.compile('<li>.*</li>')
        table = re.findall(pt, html)
        f = file(weatherpath, 'a+')
        if table!=[]:
            result = re.findall(pa, table[0])
            for i in range(6,len(result)):
                if i%6==0:
                    f.write(cnpro + '#' + cncity + '#'+cndis)
                if re.findall(r"\d{4}-\d{2}-\d{2}", result[i]) != []:
                    f.write('#'+re.findall(r"\d{4}-\d{2}-\d{2}", result[i])[0])
                else:
                    f.write('#'+result[i][4:-5].encode('utf-8')) #数据输出
                if (i+1)%6==0:
                    f.write('#'+datetime.datetime.now().strftime('%Y-%m-%d')+'\n')
            f.close()
        else:
            tof = open(failedpath, "a+")
            tof.write(cnpro + '\t' + cncity + '\t'+cndis + "\t" + date+'\t')
            tof.write(url)
            tof.write('\t'+'failed!'+"\n")
            tof.close()
    if(retry > retrytimes):
        tof = open(failedpath,"a+")
        tof.write(cnpro + '\t' + cncity + '\t'+cndis + "\t" + date+'\t')
        tof.write(url)
        tof.write('\t'+'retry failed!'+ "\n")
        tof.close()
def main():
    #citys = getcity()
    start = datetime.datetime.strptime(startdate, "%Y-%m-%d").date()
    end = datetime.datetime.strptime(enddate, "%Y-%m-%d").date()
    rundt = start
    #['省','城市','区',''日期','最高气温','最低气温','天气','风向','风力','更新日期']
    fc=open(citypath)
    city_info=fc.readlines()
    while rundt <= end:
        for line  in city_info:
            seg=line.strip().split('#')
            cnpro=seg[1]
            cncity=seg[3]
            dis=seg[4]
            cndis=seg[5]
            run = rundt.strftime('%Y%m')
            print cndis +"  "+ run
            weather(cnpro,cncity,dis,cndis,run)
            time.sleep(sleeptime)
        rundt = datetime.datetime.strptime(datetime_offset_by_month(rundt, 1).strftime('%Y-%m-%d'), "%Y-%m-%d").date()

def datetime_offset_by_month(datetime1, n = 1):
    one_day = datetime.timedelta(days = 1)
    q,r = divmod(datetime1.month + n, 12)
    datetime2 = datetime.datetime(
        datetime1.year + q, r + 1, 1) - one_day
    if datetime1.month != (datetime1 + one_day).month:
        return datetime2
    if datetime1.day >= datetime2.day:
        return datetime2
    return datetime2.replace(day = datetime1.day)

if __name__ == '__main__':
    main()
