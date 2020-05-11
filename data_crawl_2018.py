import numpy
import pandas
import requests
import json
import os
import sys
import xlrd
import csv
from datetime import datetime, timedelta

"""
부산신항 00
부산수처리장 01
부산복합자재창고 02
하동본부 10
하동보건소 11
하동변전소 12
하동정수장 13
하동공설운동장 14
하동하수처리장 15
인천수산정수장 20
"""

class dataAccess:
    def __init__(self,dataPath):
        self._dataPath=dataPath
        self._pvCrawl=pvCrawlerFile("%sPV/" % dataPath)
        self._kmaCrawl=kmaCrawlerFile("%sKMA/" % dataPath)
        self._airCrawl=airCrawlerFile("%sairKorea/" % dataPath)
        self.location_mapping()
        self.unzip_KMA()

    def clear_KMA_csv(self):
        os.system("rm %sKMA/SURFACE*.csv" % self._dataPath)

    def close(self):
        self._pvCrawl.close()
        self._airCrawl.close()
        self._kmaCrawl.close()

    def location_mapping(self):
        self._pvList = ["00","01","02","10","11","12","13","14","15","20"]
        self._locMapAir = {}
        self._locMapKMA = {}
        for i in self._pvList :
            if i[0]=="0":
                self._locMapKMA[i] = 159 #부산
                if i[1]=="0":
                    self._locMapAir[i] = "부산 사하구"
                else:
                    self._locMapAir[i] = "부산 강서구"
            elif i[0]=="1":
                self._locMapKMA[i] = 289 #경남 산청
                self._locMapAir[i] = "경남 하동군"
            elif i[0]=="2":
                self._locMapKMA[i] = 112 #인천
                self._locMapAir[i] = "인천 남동구"
    def unzip_KMA(self):
        l = list(set(self._locMapKMA.values()))
        dirList = os.listdir("%sKMA/" % self._dataPath)
        for d in dirList:
            for i in l :
                if str(i) == d[13:-22] :
                    if not os.path.exists("%sKMA/%scsv" % (self._dataPath,d[:-3])):
                        os.system("unzip %sKMA/%s -d %s/KMA" % (self._dataPath,d,self._dataPath))




    def pick_data(self,startDate,endDate,pvId,*args):
        valLists={}
        s = datetime.strptime(startDate,'%Y-%m-%d %H:%M')
        sTuple = s.timetuple()
        e = datetime.strptime(endDate, '%Y-%m-%d %H:%M')
        eTuple = e.timetuple()
        ret=[]
        for t in pandas.date_range(s,e,freq="H"):
            #tTuple = t.timetuple()
            vals = {}
            vals["time"]=str(t)
            vals["PV"]=self._pvCrawl.get_pv_value(t,pvId)
            for x in args:
                if x in ["PM10","PM25","SO2","CO","O3","NO2"] :
                    vals[x]=self._airCrawl.get_air_value(t,self._locMapAir[pvId],x)
                elif x in ["기온","강수량","습도","풍속","풍향","증기압","이슬점온도","현지기압","해면기압","일조","일사","지면온도"]:
                    vals[x]=self._kmaCrawl.get_KMA_value(t,self._locMapKMA[pvId],x)
            ret.append(vals)
        return ret

    def pick_data_all(self,pvId,*args):
        return self.pick_data("2013-01-01 01:00","2018-12-31 23:00",pvId,*args)


class pvCrawlerFile:
    def __init__(self,dataPath):
        self._path=dataPath

    def open_csv_file_as_list(self,dataId):
        csvName = dataId+".csv"
        self._f = open(self._path+csvName,'r',encoding='euc-kr')
        c = list(csv.reader(self._f))
        self.close()
        return c

    def close(self):
        self._f.close()

    def get_pv_value(self,timeStamp,pvId):
        print("get_pv_value: time:%s,pvId:%s" % (timeStamp,pvId))
        c=self.open_csv_file_as_list(pvId)
        index=c[0]
        c=c[1:]
        h=timeStamp.timetuple().tm_hour
        if h==0:
            timeStamp=timeStamp-timedelta(hours=1)
            h=h+1
        tString=str(timeStamp.strftime('%Y-%m-%d'))
        targetIndex=index.index(str(h))
        for l in c:
            if(l[0]==tString):
                ret = l[targetIndex]
                print("pvVAL : %s" % ret)
                return ret
        print("pvVAL : -1")
        return -1

class airCrawlerFile:
    def __init__(self,dataPath):
        self._path=dataPath

    def open_csv_file_as_list(self,y,m):
        def get_quarter(m):
            quarter=0
            if m<=3 :
                quarter = 1
            elif m>=10 :
                quarter = 4
            elif m<=6 :
                quarter = 2
            else :
                quarter = 3
            return quarter
        if y!=2017 and y!=2019 :
            quarter=get_quarter(m)
            if y==2013 :
                csvName = "%s년0%s분기.csv" % (y,quarter)
            elif y==2015 :
                csvName = "%s년%s분기.csv" % (y,quarter)
            else :
                csvName = "%s년 %s분기.csv" % (y,quarter)
        else:
            csvName = "%s년 %s월.csv" % (y,m)

        self._f = open("%s%s" % (self._path,csvName),'r',encoding='utf-8')
        c = list(csv.reader(self._f))
        self.close()
        return c

    def close(self):
        self._f.close()

    def get_air_value(self,timeStamp,location,arg):
        y=timeStamp.timetuple().tm_year
        m=timeStamp.timetuple().tm_mon
        c=self.open_csv_file_as_list(y,m)
        print("get_air_value: y:%s,m:%s,loc:%s,arg:%s" % (y,m,location,arg))
        index=c[0]
        targetIndex = -1
        if arg in index:
            targetIndex = index.index(arg)
        if targetIndex == -1 :
            return -1
        timeStampIndex = -1
        if '측정일시' in index:
            timeStampIndex = index.index('측정일시')
        if timeStampIndex ==-1 :
            return -1
        c=list(filter(lambda x: x[0]==location,c[1:]))
        for l in c:
            if l[timeStampIndex][-2:]!="24":
                if datetime.strptime(l[timeStampIndex],'%Y%m%d%H')==timeStamp:
                    print("airVAL : %s" % l[targetIndex])
                    return l[targetIndex]
            else:
                formattedTime = datetime.strptime(l[timeStampIndex][:-2],'%Y%m%d')+timedelta(days=1)
                if formattedTime==timeStamp:
                    print("airVAL : %s" % l[targetIndex])
                    return l[targetIndex]
        print("airVAL : -1")
        return -1


class kmaCrawlerFile:
    def __init__(self,dataPath):
        self._path=dataPath

    def open_csv_file_as_list(self,y,stnId):
        yearNameMap={'2013':"2013_2013_2015",'2014':"2014_2014_2015",'2015':"2015_2015_2016",'2016':"2016_2016_2017",'2017':"2017_2017_2018",'2018':"2018_2018_2019"}
        csvName="SURFACE_ASOS_%s_HR_%s.csv" % (stnId,yearNameMap[str(y)])
        self._f = open(self._path+csvName,'r',encoding='euc-kr')
        c = list(csv.reader(self._f))
        self.close()
        return c

    def close(self):
        self._f.close()

    def get_KMA_value(self,timeStamp,location,arg):
        y=timeStamp.timetuple().tm_year
        print("get_KMA_value: timeStamp:%s,loc:%s,arg:%s" % (timeStamp,location,arg))
        c=self.open_csv_file_as_list(y,location)
        index=c[0]
        targetIndex = -1
        for argx in index:
            if arg in argx:
                targetIndex = index.index(argx)
                break;
        if targetIndex == -1 :
            return -1
        c=c[1:]
        for l in c:
            if datetime.strptime(l[1],'%Y-%m-%d %H:%M') == timeStamp:
                ret = l[targetIndex]
                print("KMAVAL : %s" % ret)
                return ret
        print("KMAVAL : -1")
        return -1







if __name__ == "__main__":
    def save_as_csv(returnList):
        with open("db.csv", 'w', newline='') as f:
            wr = csv.writer(f, quoting=csv.QUOTE_ALL)
            wr.writerow(returnList)

    dataSetPath=os.getcwd()+"/data/"
    access=dataAccess(dataSetPath)
    pvList = ["00","01","02","10","11","12","13","14","15","20"]
    for x in pvList:
        print("accessing PV %s" % x)
        print("writing year 2018")
        retList = access.pick_data("2018-01-01 01:00", "2018-12-31 23:00",x,
                                   "PM10","PM25","SO2","CO","O3","NO2",
                                   "기온","강수량","습도","풍속","풍향","증기압","이슬점온도","현지기압","해면기압","일조","일사","지면온도")
        keys = retList[0].keys()
        try :
            output_file=open('%s_db_2018.csv' % x, 'w',newline='')
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(retList)
        except Exception as e:
            print(e)
            output_file.close()
    print("year 2018 complete")

























#    dataKey = "jq27XGJNTkYKLuRiOklVjaJ9sVwsLWCFVxFaaFzX3mywbfSiirE/yMHVDMSBYDJ2YmUbnhYtdZXHZHQHW3L6jg=="

#    pv=pvCrawler()
#    air=airCrawlerAPI(dataKey)
#    kma=kmaCrawlerAPI(dataKey)
#    ret = kma.reqASOSHourly('20190101','01','20190104','01','108')
#    print(ret.content)
#    print(json.dumps(ret.json(), indent=4, sort_keys=True))



"""
class airCrawlerAPI:
    def __init__(self,apiKey):
        self._key=apiKey



class kmaCrawlerAPI:
    def __init__(self,apiKey):
        self._key=apiKey

    def reqASOSHourly(self,startDt,startHh,endDt,endHh,stnId) :
        print(self._key);
        params={
        'serviceKey' : self._key,
        'numOfRows' : '100',
        'schListCnt' : '100',
        'pageNo' : '1',
        'dataType' : 'JSON',
        'dataCd' : 'ASOS',
        'dateCd' : 'HR',
        'startDt' : startDt,
        'startHh' : startHh,
        'endDt' : endDt,
        'endHh' : endHh,
        'stnIds' : stnId,
        }
        res = requests.get('http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList', params=params)
        return res
"""

