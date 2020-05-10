import csv
import pandas
from datetime import datetime,timedelta
import os
import json


class dbAccess:
    def __init__(self,dbPath):
        self._path = dbPath

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
        print(arg)
        for x in arg :
            targetIndex = -1
            if x in index :
                targetIndex = index.index(x)
            if targetIndex == -1 :
                continue
            dic={"idx":targetIndex,"arg":x}
            argIndex.append(dic)
        print(argIndex)

        for t in pandas.date_range(s,e,freq="H"):
            vals = {}
            vals["time"]=str(t)
            for dic in argIndex:
                print(dic)
                vals[dic["arg"]]=c[tIndex][dic["idx"]]
            ret.append(vals)
            tIndex=tIndex+1
        return ret


if __name__ == "__main__":
    pvId = input("pvID : ")
    year = input("year : ")
    db = dbAccess("%s_db_%s.csv" % (pvId,year))
    print(db.get_data("2013-01-01 01:00", "2013-01-02 01:00", "PV","PM10","PM25","일사"))
