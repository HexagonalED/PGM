import csv
import pandas
from datetime import datetime,timedelta
import os
import json
import urllib
import requests as rq


class dbAccess:
    def __init__(self,dbPath,apiId,pvId):
        self._path = dbPath
        self._id = apiId
        self._pvId=pvId
        if pvId[0]=="0":
            self._locMapKMA = 159 #부산
        elif pvId[0]=="1":
            self._locMapKMA = 192 #진주(경남 산청에는 일사량 측정 불가)
        elif pvId[0]=="2":
            self._locMapKMA = 112 #인천
        self._url="http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList?serviceKey="+apiId+"&dataType=JSON&dataCd=ASOS&dateCd=HR&numOfRows=24&schListCnt=24&pageNo=1&stnIds="+str(self._locMapKMA)

    def get_data(self,startDate,endDate,*arg):
        f = open(self._path, 'r', encoding='utf-8')
        c = list(csv.reader(f))
        index = c[0]
        c=c[1:]
        s = datetime.strptime(startDate,'%Y-%m-%d %H:%M')
        sTuple = s.timetuple()
        e = datetime.strptime(endDate, '%Y-%m-%d %H:%M')
        eTuple = e.timetuple()
        ret = []
        startIndex = 0
        for l in c:
            if datetime.strptime(l[0],'%Y-%m-%d %H:%M:%S') == s :
                break
            else : startIndex=startIndex+1
        tIndex = startIndex
        argIndex=[]
        #print(arg)
        for x in arg :
            if x == "일사" :
                argIndex.append({"dic":-1,"arg":x})
                continue
            targetIndex = -1
            if x in index :
                targetIndex = index.index(x)
            if targetIndex == -1 :
                continue
            dic={"idx":targetIndex,"arg":x}
            argIndex.append(dic)
        #print(argIndex)
        for t in pandas.date_range(s,e,freq="H"):
            vals = {}
            vals["time"]=str(t)
            for dic in argIndex:
                #print(dic)
                if dic["arg"] == "일사":
                    #call API
                    tIlsa=datetime.strptime(str(t),"%Y-%m-%d %H:%M:%S")
                    ilsaYM = str(tIlsa.year)+("%02d"%tIlsa.month)+("%02d"%tIlsa.day)
                    ilsaHR = "%02d"%tIlsa.hour
                    reqUrl = self._url+"&startDt=%s&startHh=%s&endDt=%s&endHh=%s" % (ilsaYM,ilsaHR,ilsaYM,ilsaHR)
                    res = rq.get(reqUrl).json()
                    #print(reqUrl)
                    vals[dic["arg"]]=res["response"]["body"]["items"]["item"][0]["icsr"]
                else:
                    vals[dic["arg"]]=c[tIndex][dic["idx"]]
            ret.append(vals)
            tIndex=tIndex+1
        return ret


if __name__ == "__main__":
    pvId = input("pvID : ")
    year = input("year : ")
    db = dbAccess("%s_db_%s.csv" % (pvId,year),"jq27XGJNTkYKLuRiOklVjaJ9sVwsLWCFVxFaaFzX3mywbfSiirE%2FyMHVDMSBYDJ2YmUbnhYtdZXHZHQHW3L6jg%3D%3D",pvId)
    print(db.get_data("2013-01-01 01:00", "2013-01-02 01:00", "PV","PM10","PM25","일사"))
