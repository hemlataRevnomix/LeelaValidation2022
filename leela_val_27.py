import os
import file_check as fc
import pandas as pd

from datetime import datetime,timedelta,date

today1 = date.today()
d1 = today1.strftime("%Y-%m-%d")
xDate = d1
basepath = r'C:\ftp'
inpath = basepath + r'\email_data/'+d1
outputPath = r'C:\Chakradhar\hemlata\Validation project\leela\Validation Reports'
htl_list=['blr','chn','mum','ggn','goa','lach','lbc','tlgn','tlpj','tlpnd','udp']
today = datetime.today().date()
xDate = str(today)

reports = ["Financial", "Group", "Reservation_Future","Reservation_Hist"]

os.chdir(outputPath)

try:
    os.mkdir(outputPath)
except FileExistsError:
    pass

for cl_htl in htl_list:

    for lst in os.listdir(inpath):
        file = open(outputPath + '/ ' + '{}_File Validation.txt'.
                    format(cl_htl), "w")
        for report in reports:
            if lst.lower().__contains__(f'leela{cl_htl}_Daily_{report}'.lower()):
                fin = inpath + "\\" + lst

        # if lst.startswith(f'leela{cl_htl}_Daily_Group'):
        #     grp = inpath + "\\" + lst
        # if lst.startswith(f'leela{cl_htl}_Daily_Reservation_Future'):
        #     res_fut = inpath + "\\" + lst
        # if lst.startswith(f'leela{cl_htl}_Daily_Reservation_Historical'):
        #     res_hist = inpath + "\\" + lst



    # inPath = basePath + '\\' + htlCode
    # try:
    #     fin = inpath + '\\' + financial
    #     grp = inpath + '\\' + group
    #     res_f = inpath + '\\' + res_fut
    #     res_h = inpath + '\\' + res_hist
    # except:
    #     pass

                findf=fc.fileCheck(fin,report,file,xDate, cl_htl)
                print(f"{cl_htl} {report} file Checked")
            # grpdf=fc.fileCheck(grp,'Group',file,xDate)
            # print("{} group file Checked".format(cl_htl))
            # res_fdf=fc.fileCheck(res_fut,'Reservation_Future',file,xDate)
            # print("{} Reservation_Future file Checked".format(cl_htl))
            # res_hdf=fc.fileCheck(res_hist,'Reservation_Hist',file,xDate)
            # print("{} Reservation_History file Checked".format(cl_htl))
            # else:
            #     fin = None
            #     findf = fc.fileCheck(fin, report, file, xDate, cl_htl)
            #     print(f"{cl_htl} {report} file Checked")



