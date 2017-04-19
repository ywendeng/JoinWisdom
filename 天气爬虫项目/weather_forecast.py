# -*- encoding:utf8 -*-
import re,urllib,time,socket
from setting_forecast import weatherpath,failedpath,deftimeout,sleeptime,retrytimes,citypath,cityfromfile
from datetime import datetime

# 设置超时时间为10秒
socket.setdefaulttimeout(deftimeout)
def weather(province,city,district):
    propinyin=province[0]
    cnpro=province[1]
    citypinyin=city[0]
    cncity=city[1]
    dispinyin=district[0]
    cndis=district[1]
    update_dt=datetime.now().strftime("%Y-%m-%d")
    url = "http://"+citypinyin+".tianqi.com/"+dispinyin+"/30/"
    #print url
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
            print cncity + "  " + " 超时重试..."
            time.sleep(1)
        pat = re.compile('<div class="today_30t.*</div>', re.S)  # 提取整张表格
        tablers = re.findall(pat, html)  # 获取表格结果
        f=open(weatherpath,'a+')
        if tablers != []:
            table = tablers[0]
            pt = re.compile("\s<h4>(.*?)</ul>\s*<ul>(.*?)</ul>", re.S)
            data = re.findall(pt, table)
                # print data[0][0],data[0][1]
            span = re.compile('<span>(.*?)</span>', re.S)
            li = re.compile('[345]">(.*?)</li', re.S)
            for i in range(len(data)):
                date = re.findall(span, data[i][0])[0].split(' ')[0].encode('utf-8')
                month=re.findall('\d+',date)[0]
                day=re.findall('\d+',date)[1]
                year=datetime.now().strftime('%Y')
                d=year+'-'+month+'-'+day
                if len(re.findall(span, data[i][0])[0].split(' '))>2:
                    weekday=re.findall(span, data[i][0])[0].split(' ')[2][-10:-7].encode('utf-8')
                else:
                    weekday=re.findall(span, data[i][0])[0].split(' ')[1].encode('utf-8')
		day_weather = re.findall(span, data[i][0])[1].encode('utf-8')
                day_temperature = re.findall(li, data[i][0])[0].split('>')[1][:-6].encode('utf-8')
                day_wind_force = re.findall(li, data[i][0])[2].encode('utf-8')
                day_wind_direction = re.findall(li, data[i][0])[1].encode('utf-8')
                night_weather = re.findall(span, data[i][1])[0].encode('utf-8')
                night_temperature = re.findall(li, data[i][1])[0].split('>')[1][:-6].encode('utf-8')
                night_wind_force = re.findall(li, data[i][1])[2].encode('utf-8')
                night_wind_direction = re.findall(li, data[i][1])[1].encode('utf-8')
                f.write('#'.join([cnpro,cncity,cndis,d, weekday, day_weather, day_temperature, day_wind_force, day_wind_direction, night_weather,
                         night_temperature, night_wind_force, night_wind_direction,update_dt]))
                f.write('\n')
            f.close()
        else:
            tof = open(failedpath, "a+")
            tof.write(cnpro+'\t'+cncity+ "\t" +cndis+'\t'+url+'\t')
            tof.write('failed!'+"\n")
            tof.close()
    if(retry > retrytimes):
        tof = open(failedpath,"a+")
        tof.write(cnpro+'\t'+cncity + "\t" +cndis+'\t'+url + "\t")
        tof.write('\t'+'retry failed!'+ "\n")
        tof.close()
def getcity():
    # 获取主要城市
    page = urllib.urlopen("http://www.tianqi.com/province/")
    html = page.read().decode("gbk")
    city_dict = {}
    # 获取34个省份的拼音,中文名
    propat = re.compile('<div class="select">(.*?)</select>', re.S)
    subpropat = re.compile('py=.*<')
    province_info = re.findall(propat, html)
    province_list = []
    if province_info != []:
        provinces = province_info[0]
        province_names = re.findall(subpropat, provinces)
        for p in province_names:
            province_list.append((p.split('>')[0][4:-1], p.split('>')[1][2:-1]))
    # 获取地级市的拼音，中文名
    for propinyin, cnpro in province_list:
        key1 = (propinyin, cnpro)
        city_dict[key1] = {}
        url = "http://www.tianqi.com/province/" + propinyin + "/"
        page = urllib.urlopen(url)
        html = page.read().decode("gbk")
        # print html
        citypat = re.compile('id="city">(.*?)</select>', re.S)
        citypat2 = re.compile('py=.*</o')
        city_list = [(c.split('">')[0][4:-10], c.split('">')[1][2:-3]) for c in
                     re.findall(citypat2, re.findall(citypat, html)[0])]
        for citypinyin, cncity in city_list:
            key2 = (citypinyin, cncity)
            city_dict[key1][key2] = []
            url = 'http://' + citypinyin + '.tianqi.com/'
            page = urllib.urlopen(url)
            html = page.read().decode("gbk")
            dispat = re.compile('id="zone">(.*?)</select>', re.S)
            value = [(d.split('>')[0][4:-11], d.split('>')[1][2:-3]) for d in
                     re.findall(citypat2, re.findall(dispat, html)[0])]
            city_dict[key1][key2].append(value)

    for province in city_dict.keys():
        citys=city_dict[province]
        for city in citys.keys():
            for district in citys[city]:
                for d in district:
                    fc = open(citypath, 'a+')
                    fc.write('#'.join([province[0].encode('utf-8'),province[1].encode('utf-8'),city[0].encode('utf-8'),
                                   city[1].encode('utf-8'),d[0].encode('utf-8'),d[1].encode('utf-8')]))
                    fc.write('\n')

def main():
    if cityfromfile:
        # 打印表头
        #f.write('#'.join(
            #['province', 'city', 'district', 'date', 'weekday', 'day_weather', 'day_temperature', 'day_wind_direction',
            #'day_wind_force', 'night_weather', 'night_temperature', 'night_wind_direction', 'night_wind_force',
            #'fc_dt']))
        fc=open(citypath)
        citys_info=fc.readlines()
        for line in citys_info:
            seg=line.strip().split('#')
            province=(seg[0],seg[1])
            city=(seg[2],seg[3])
            district=(seg[4],seg[5])
            #print province[1]+'\t'+city[1]+'\t'+district[1]
            weather(province,city,district)
            time.sleep(sleeptime)
        fc.close()
    else:
        getcity()

if __name__ == '__main__':
    main()
