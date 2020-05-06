import xlrd
import os
import csv


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

def xlsx_to_csv(xlsxName) :
    wb = xlrd.open_workbook(xlsxName)
    sh = wb.sheet_by_name(wb.sheet_names()[0])
    with open("%scsv" % xlsxName[:-4], 'w', encoding='utf8') as nc:
        wr = csv.writer(nc, quoting=csv.QUOTE_ALL)
        for rownum in range(sh.nrows):
            wr.writerow(sh.row_values(rownum))
    return


if __name__ == "__main__":
    #xltocsv
    dataSetPath=os.getcwd()+"/data/"
    os.chdir("%sairKorea/" % dataSetPath)
    dList=os.listdir()
    for d in dList:
        if ".zip" not in d:
            print("visiting %s" % d)
            os.chdir(d)
            fList=os.listdir()
            for f in fList:
                print("checking %s" % f)
                if ".xlsx" in f:
                    print("converting %s" % f)
                    xlsx_to_csv("%s/%s" % (os.getcwd(),f))
            os.chdir("..")
