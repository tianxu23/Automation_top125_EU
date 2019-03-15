import decimal
import pyodbc
import time
import csv
import datetime
import os
import sys
import subprocess
import numpy
import email
import smtplib
import shutil

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
from datetime import datetime as dt

os.chdir('C:/Users/tianxu/Documents/top125/ExcelEU')
pyodbc.pooling = False

#def main():
Login_info = open('C:/Work/LogInMozart_ts.txt', 'r')
server_name = Login_info.readline()
server_name = server_name[:server_name.index(';')+1]
UID = Login_info.readline()
UID = UID[:UID.index(';') + 1]
PWD = Login_info.readline()
PWD = PWD[:PWD.index(';') + 1]
Login_info.close()
#today_dt = datetime.date.today()
print 'Connecting Server to determine date info at: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '.'
conn = pyodbc.connect('DRIVER={Teradata};DBCNAME='+ server_name +'UID=' + UID + 'PWD=' + PWD)
curs = conn.cursor()
curs.execute('''
    SELECT
        MAX(CK_TRANS_DT)
    FROM PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT
    WHERE CK_TRANS_DT >= CURRENT_DATE - 10
''')
end_dt1 = curs.fetchall()[0][0]

curs.execute('''
    SELECT
        MAX(TRANS_DT)
    FROM prs_ams_v.AMS_PBLSHR_ERNG
    WHERE TRANS_DT >= CURRENT_DATE - 10
''')
end_dt2 = curs.fetchall()[0][0]

curs.execute('''
    SELECT
        MAX(CLICK_DT)
    FROM PRS_AMS_V.AMS_CLICK
    WHERE CLICK_DT >= CURRENT_DATE - 10
''')
end_dt3 = curs.fetchall()[0][0]

#text_file = open("date.txt", "r")
#record_time = datetime.datetime.strptime(text_file.readlines()[0], '%Y-%m-%d').date()
current_date = datetime.now().date()
Day = timedelta(2)

print 'Table date: ' + end_dt1.strftime("%Y-%m-%d")

curs.execute('''
delete from  p_chengliu_t.B2C
where ck_trans_dt between current_date-40 and current_date
OR ck_trans_dt between current_date-100 and current_date-90
OR ck_trans_dt between current_date-375 and current_date-365
'''
)
conn.commit()

curs.execute('''
CREATE MULTISET VOLATILE TABLE FAM_B2C2 AS
(
SELECT
A.CK_TRANS_DT,
AMS_PRGRM_ID,
A.EPN_PBLSHR_ID,
  COUNT(DISTINCT CK_TRANS_ID||ITEM_ID) AS TRANS,
  SUM(IGMB_PLAN_RATE_AMT) AS iGMB,
  SUM(IREV_PLAN_RATE_AMT) AS iREV
FROM
  PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT A
LEFT OUTER JOIN
  ACCESS_VIEWS.DW_USERS AS B ON A.SELLER_ID = B.USER_ID
LEFT OUTER JOIN
  PRS_RESTRICTED_V.CUST_SLR_SGMNTN_HIST AS C
ON A.SELLER_ID = C.SLR_ID
AND A.CK_TRANS_DT BETWEEN C.CUST_SLR_SGMNTN_BEG_DT AND C.CUST_SLR_SGMNTN_END_DT
AND C.CUST_SGMNTN_GRP_CD BETWEEN 36 AND 41
AND C.CUST_SGMNTN_CD IN (1,7,13,19,25,31,2,8,14,20,26,32,3,9,15,21,27,33)
WHERE
(A.CK_TRANS_DT BETWEEN CURRENT_DATE -40 AND CURRENT_DATE
	or A.CK_TRANS_DT BETWEEN CURRENT_DATE -100 AND CURRENT_DATE-90
    or A.CK_TRANS_DT BETWEEN CURRENT_DATE -375 AND CURRENT_DATE-365)
AND MPX_CHNL_ID = 6
and (B.USER_SITE_ID IN (0,100,15) AND C.SLR_ID IS NOT NULL 
OR B.USER_SITE_ID NOT IN (0,100,15) AND B.USER_DSGNTN_ID =  2 )
GROUP BY 1,2,3
 ) WITH DATA PRIMARY INDEX ( AMS_PRGRM_ID , EPN_PBLSHR_ID, CK_TRANS_DT ) ON COMMIT PRESERVE ROWS;
''')
conn.commit()

curs.execute('''
--CREATE MULTISET TABLE P_tiansheng_T.FAM_B2C_try3 AS(
insert into p_chengliu_t.B2C
SELECT AMS_PRGRM_ID , EPN_PBLSHR_ID , CK_TRANS_DT, SUM ( iGMB ) AS iGMB , SUM ( iRev ) AS iRev , SUM ( TRANS) AS Trans 
 FROM FAM_B2C2
 GROUP BY 1 , 2 , 3
--) WITH DATA PRIMARY INDEX ( AMS_PRGRM_ID , EPN_PBLSHR_ID, CK_TRANS_DT );

''')
conn.commit()

curs.execute('''
create multiset volatile table pub_spend as(	
select
  TRANS_DT,
  AMS_PRGRM_ID,
  AMS_PBLSHR_ID,
  sum(COALESCE(ERNG_USD,0.00) ) as Spend
from	prs_ams_v.AMS_PBLSHR_ERNG A
where
  TRANS_DT between current_date-40 and current_date
  or
  TRANS_DT between current_date-100 and current_date-90
  or
  TRANS_DT between current_date-375 and current_date-365

group by 1,2,3
)with data primary index (AMS_PBLSHR_ID,TRANS_DT,AMS_PRGRM_ID) on commit preserve rows;
''')
conn.commit()


curs.execute('''

CREATE MULTISET VOLATILE TABLE pub_clk AS(
SEL 
a.CLICK_dt,
 AMS_PRGRM_ID,
a.PBLSHR_ID  ,
count(click_id) as click_all
FROM PRS_AMS_V.AMS_CLICK a
WHERE a. AMS_TRANS_RSN_CD=0
AND 
(CLICK_dt between  current_date-40 and current_date
	or
CLICK_dt between  current_date-100 and current_date-90
	or
CLICK_dt between  current_date-375 and current_date-365
)

group by 1,2,3
) WITH DATA PRIMARY INDEX ( PBLSHR_ID , click_dt, AMS_PRGRM_ID)ON COMMIT PRESERVE ROWS;

''')
conn.commit()

curs.execute('''
CREATE MULTISET VOLATILE TABLE pub_fam AS
(select
  CK_TRANS_DT,
 AMS_PRGRM_ID,
a.EPN_PBLSHR_ID,
  count(distinct CASE WHEN DEVICE_TYPE_ID IN (2,3) THEN 0 ELSE CK_TRANS_ID||ITEM_ID END) AS trans_Desktop,
  count(distinct CASE WHEN DEVICE_TYPE_ID IN (2,3) THEN  CK_TRANS_ID||ITEM_ID ELSE 0 END) AS trans_Mobile,
  SUM(CASE WHEN DEVICE_TYPE_ID IN (2,3) THEN 0 ELSE IGMB_PLAN_RATE_AMT END) AS iGMB_Desktop,
  SUM(CASE WHEN DEVICE_TYPE_ID IN (2,3) THEN IGMB_PLAN_RATE_AMT ELSE 0 END) AS iGMB_Mobile,
  SUM(CASE WHEN DEVICE_TYPE_ID IN (2,3) THEN 0 ELSE IREV_PLAN_RATE_AMT END) AS iREV_Desktop,
  SUM(CASE WHEN DEVICE_TYPE_ID IN (2,3) THEN IREV_PLAN_RATE_AMT ELSE 0 END) AS iREV_Mobile  
from
  PRS_RESTRICTED_V.MH_IM_CORE_FAM2_FACT a
where
  a.MPX_CHNL_ID=6
 and 
 (CK_TRANS_DT between  current_date - 40 and current_date
 OR
 CK_TRANS_DT between  current_date - 100 and current_date -90
 OR
 CK_TRANS_DT between  current_date - 375 and current_date -365
)
group by 1,2,3
) WITH DATA PRIMARY INDEX ( EPN_PBLSHR_ID, CK_TRANS_DT, AMS_PRGRM_ID)ON COMMIT PRESERVE ROWS;
''')
conn.commit()



curs.execute('''
CREATE MULTISET VOLATILE TABLE pub_daily_pfm_ab AS(
select
  	coalesce(a.CLICK_dt, b.TRANS_DT) as TRANS_DT,
  	coalesce(a.AMS_PRGRM_ID,b.AMS_PRGRM_ID) as AMS_PRGRM_ID,
  	coalesce(a.PBLSHR_ID,b.AMS_PBLSHR_ID) as pblshr_id,
  coalesce(SPEND,0) as Spend,
  coalesce(click_all,0) as click_all
from      pub_clk a 
full join  pub_spend b on a.CLICK_dt=b.TRANS_DT and a.AMS_PRGRM_ID=b.AMS_PRGRM_ID and a.PBLSHR_ID=b.AMS_PBLSHR_ID 
group by 1,2,3,4,5
) WITH DATA PRIMARY INDEX ( pblshr_id, TRANS_DT, AMS_PRGRM_ID)ON COMMIT PRESERVE ROWS;
''')
conn.commit()

curs.execute('''
CREATE MULTISET VOLATILE TABLE pub_daily_pfm_bc AS(
select
  	coalesce( b.TRANS_DT,c.CK_TRANS_DT) as TRANS_DT,
  	coalesce(b.AMS_PRGRM_ID,c.AMS_PRGRM_ID) as AMS_PRGRM_ID,
  	coalesce(b.AMS_PBLSHR_ID,c.EPN_PBLSHR_ID) as pblshr_id,
  coalesce( trans_Desktop,0) as trans_Desktop,
  coalesce( trans_Mobile,0) as trans_Mobile,
  coalesce(iGMB_Desktop,0) as iGMB_Desktop,
  coalesce(iGMB_Mobile,0) as iGMB_Mobile,
  coalesce(iGMB_Desktop,0) as iREV_Desktop,
  coalesce(iGMB_Mobile,0) as iREV_Mobile,
  coalesce(SPEND,0) as Spend
from      pub_spend b 
full join  pub_fam c on b.TRANS_DT=c.CK_TRANS_DT and b.AMS_PRGRM_ID=c.AMS_PRGRM_ID and b.AMS_PBLSHR_ID=c.EPN_PBLSHR_ID 
group by 1,2,3,4,5,6,7,8,9,10
) WITH DATA PRIMARY INDEX ( pblshr_id, TRANS_DT, AMS_PRGRM_ID)ON COMMIT PRESERVE ROWS;
''')
conn.commit()

curs.execute('''
CREATE MULTISET VOLATILE TABLE pub_daily_pfm_abc AS(
select
  	coalesce( b.TRANS_DT,c.TRANS_DT) as TRANS_DT,
  	coalesce(b.AMS_PRGRM_ID,c.AMS_PRGRM_ID) as AMS_PRGRM_ID,
  	coalesce(b.pblshr_id,c.pblshr_id) as pblshr_id,
  coalesce( trans_Desktop,0) as trans_Desktop,
  coalesce( trans_Mobile,0) as trans_Mobile,
  coalesce(iGMB_Desktop,0) as iGMB_Desktop,
  coalesce(iGMB_Mobile,0) as iGMB_Mobile,
    coalesce(iREV_Desktop,0) as iREV_Desktop,
  coalesce(iREV_Mobile,0) as iREV_Mobile,
  coalesce(b.SPEND,c.spend) as Spend,
  coalesce(click_all,0) as click_all
from      pub_daily_pfm_ab b 
full join  pub_daily_pfm_bc c on b.TRANS_DT=c.TRANS_DT and b.AMS_PRGRM_ID=c.AMS_PRGRM_ID and b.pblshr_id=c.pblshr_id 
group by 1,2,3,4,5,6,7,8,9,10,11
) WITH DATA PRIMARY INDEX ( pblshr_id, TRANS_DT, AMS_PRGRM_ID)ON COMMIT PRESERVE ROWS;
''')
conn.commit()

curs.execute('''
delete from  P_ePNPEM_T.mbai_pub_daily_pfm_0306 
where TRANS_DT between current_date-40 and current_date
OR TRANS_DT between current_date-100 and current_date-90
OR TRANS_DT between current_date-375 and current_date-365

;
''')
conn.commit()

curs.execute('''
insert into P_ePNPEM_T.mbai_pub_daily_pfm_0306 
select
TRANS_DT                      
,AMS_PRGRM_ID                  
,pblshr_id                     
,trans_Desktop  AS      fam2_trans_desktop          
,trans_Mobile   AS        fam3_trans_mobile       
,iGMB_Desktop   AS       fam2_igmb_desktop        
,iGMB_Mobile     AS      fam3_igmb_mobile        
,Spend                         
,click_all                     
,iREV_Desktop   AS     fam2_irev_desktop          
,iREV_Mobile  AS fam3_irev_mobile
from   pub_daily_pfm_abc
where TRANS_DT between current_date-40 and current_date
OR TRANS_DT between current_date-100 and current_date-90
OR TRANS_DT between current_date-375 and current_date-365
;
''')
conn.commit()


#print 'Record date: ' + record_time.strftime("%Y-%m-%d")
#text_file.close()
if (end_dt1 < current_date - Day and end_dt2 < current_date - Day and end_dt3 < current_date - Day):
    print 'Data is not fully ready.'
    print 'Send eMail'
    execfile('EmailSender_Traffic.py')
    conn.close()
    exit(1)
else:
    print 'Updating Excel file'
    for cn in ['ES', 'FR', 'IT', 'UK', 'DE', 'FRITES']:
    #for cn in ['ES', 'FR']:
        # print 'Processing ' + cn + ' file'
        # print 'starting time: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        p = subprocess.Popen('\"C:/Program Files (x86)/Microsoft Office/Office16/excel.exe\" \"C:/Users/tianxu/Documents/top125/ExcelEU/driver' + cn + '.xlsm\"', shell=True, stdout = subprocess.PIPE)
        stdout, stderr = p.communicate()
        print stdout
        print 'Update Finished: ' + cn
        time.sleep(10)
        # print 'Ending time: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print 'Send eMail'
        execfile('EmailSender_Traffic1.py')
    conn.close()
    exit(0)