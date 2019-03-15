import decimal
import pyodbc
import time
import csv
import datetime
import os
import sys
import subprocess
import numpy
import utils
import email
import smtplib
import shutil

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
os.chdir('C:/Work/2016_Han/TOP 125 Report/ExcelEU')
pyodbc.pooling = False

#def main():
Login_info = open('C:/Work/LogInMozart_han.txt', 'r')
server_name = Login_info.readline()
server_name = server_name[:server_name.index(';')+1]
UID = Login_info.readline()
UID = UID[:UID.index(';') + 1]
PWD = Login_info.readline()
PWD = PWD[:PWD.index(';') + 1]
Login_info.close()
today_dt = datetime.date.today()
print 'Connecting Server to determine date info at: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '.'
conn = pyodbc.connect('DRIVER={Teradata};DBCNAME='+ server_name +'UID=' + UID + 'PWD=' + PWD)
curs = conn.cursor()
curs.execute('''
    SELECT
        MAX(trans_dt)
    FROM app_mrktng_l2_v.performance
    WHERE trans_dt >= CURRENT_DATE - 10
''')
end_dt = curs.fetchall()[0][0]

text_file = open("date.txt", "r")
record_time = datetime.datetime.strptime(text_file.readlines()[0], '%Y-%m-%d').date()
print 'Table date: ' + end_dt.strftime("%Y-%m-%d")
print 'Record date: ' + record_time.strftime("%Y-%m-%d")
text_file.close()
if end_dt < record_time:
    print 'Data is not fully ready.'
    conn.close()
    exit(1)
else:
    print 'Updating WMQ list'
    curs.execute('''
        SELECT
            MAX(qtr_beg_dt)
            ,MAX(qtr_end_dt)
        FROM dw_cal_dt
        WHERE qtr_of_cal_id =
        (
            SELECT
                qtr_of_cal_id
            FROM dw_cal_dt
        WHERE cal_dt = \'''' + end_dt.strftime('%Y-%m-%d') + '''\'
        ) - 1
    ''')
    raw_data = curs.fetchall()
    QTR_bg_dt = raw_data[0][0]
    QTR_ed_dt = raw_data[0][1]
    print 'Quarter data range: ' + QTR_bg_dt.strftime('%Y-%m-%d') + ' and ' + QTR_ed_dt.strftime('%Y-%m-%d')
    print 'Getting table date'
    curs.execute('''
        SELECT
            MAX(end_dt)
        FROM p_chengliu_t.top125_WMQ_list
    ''')
    raw_data = curs.fetchall()
    table_dt = raw_data[0][0]
    print table_dt.strftime('%Y-%m-%d')
    if QTR_ed_dt > table_dt:
        print 'Updating the list'
        curs.execute('''
            DELETE FROM p_chengliu_t.top125_WMQ_list;
            insert p_chengliu_t.TOP125_WMQ_LIST
            select
                A.AMS_PRGRM_ID,
                A.PBLSHR_ID,
                B.PBLSHR_CMPNY_NAME,
                coalesce(C.MANUAL_BM, 'Unknown') as "BM",
                coalesce(C.MANUAL_SUB_BM, 'Unknown') as "Sub BM",
                max(A.TRANS_DT) as END_DT,
                sum(zeroifnull(A.FAM2_iGMB_DESKTOP))+sum(zeroifnull(A.FAM3_iGMB_MOBILE)) as iGMB,
                row_number() over (partition by AMS_PRGRM_ID order by iGMB desc) as RK
            from
                app_mrktng_l2_v.PERFORMANCE A
                left join prs_ams_v.AMS_PBLSHR B
                    on A.PBLSHR_ID = B.AMS_PBLSHR_ID
                left join p_chengliu_t.NEW_BM C
                    on A.PBLSHR_ID = C.AMS_PBLSHR_ID
            where
                A.TRANS_DT between \'''' + QTR_bg_dt.strftime('%Y-%m-%d') + '''\' and \'''' + QTR_ed_dt.strftime('%Y-%m-%d') + '''\'
                and A.PBLSHR_ID <> -999
            GROUP BY 1, 2,  3, 4, 5
        ''')
        conn.commit()
    else:
        print 'The list will not be updated.'

    #for cn in ['ES', 'FR', 'IT', 'UK','DE']:
    for cn in ['ES', 'FR']:
        # print 'Processing ' + cn + ' file'
        # print 'starting time: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        p = subprocess.Popen('\"C:/Program Files (x86)/Microsoft Office/Office16/excel.exe\" \"C:/Work/2016_Han/TOP 125 Report/ExcelEU/driver' + cn + '.xlsm\"', shell=True, stdout = subprocess.PIPE)
        stdout, stderr = p.communicate()
        print stdout
        time.sleep(10)
        # print 'Ending time: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    curs.execute('''
        SELECT
            rtl_week_beg_dt + 6
        FROM dw_cal_dt
        WHERE cal_dt = DATE\'''' + end_dt.strftime("%Y-%m-%d") + '''\' + 1
    ''')
    upd_dt = curs.fetchall()[0][0]
    file = open("date.txt", "w")
    file.write(upd_dt.strftime("%Y-%m-%d"))
    file.close()
    conn.close()