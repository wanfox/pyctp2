# -*- coding: utf-8 -*-

from core.base import (BaseObject,)
from core.utils import (fcustom)
import logging
import re

SOURCE_DATA_PATH = u'data/tradeblazer/'

fname_pattern_m1 = SOURCE_DATA_PATH + u'min1/%s(1分钟).csv'


def extractor_m1(line):
    '''
        用于无小数价位的合约
    '''
    items = re.split(',| ',line)
    record = BaseObject()
    record.idate = int(items[0].replace('/',''))
    record.imin = int(items[1].replace(':',''))
    record.iopen = int(items[2])
    record.ihigh = int(items[3])
    record.ilow = int(items[4])
    record.iclose = int(items[5])
    record.ivolume = int(items[6]) 
    record.iholding = int(items[7]) 
    record.itype = False
    return record
    
def extractor_m1_10(line):
    '''
        仅用于IF或带一位小数价位的合约
    '''
    items = re.split(',| ',line)
    record = BaseObject()
    record.idate = int(items[0].replace('/',''))
    record.imin = int(items[1].replace(':',''))
    record.iopen = int(float(items[2])*10 + 0.1)
    record.ihigh = int(float(items[3])*10 + 0.1)
    record.ilow = int(float(items[4])*10 + 0.1)
    record.iclose = int(float(items[5])*10 + 0.1)
    record.ivolume = int(float(items[6]) + 0.1)
    record.iholding = int(float(items[7]) + 0.1)
    record.itype = False
    return record

def extractor_m1_100(line):
    '''
        仅用于IF或带一位小数价位的合约
    '''
    items = re.split(',| ',line)
    record = BaseObject()
    record.idate = int(items[0].replace('/',''))
    record.imin = int(items[1].replace(':',''))
    record.iopen = int(float(items[2])*100 + 0.1)
    record.ihigh = int(float(items[3])*100 + 0.1)
    record.ilow = int(float(items[4])*100 + 0.1)
    record.iclose = int(float(items[5])*100 + 0.1)
    record.ivolume = int(float(items[6]) + 0.1)
    record.iholding = int(float(items[7]) + 0.1)
    record.itype = False
    return record

def read_min1(cname,extractor=extractor_m1,tfrom=0,tto=99999999):
    '''
        cname:合约名
    '''
    records = []
    filename = fname_pattern_m1 % (cname,)
    try:
        with open(filename,'r') as f:
            data = f.read()
        lines = data.split('\n')
        for line in lines:
            if len(line.strip()) > 0:
                record = extractor(line)
                if record.idate >=tfrom and record.idate < tto:
                    records.append(record)
    except Exception as inst:#读不到数据,默认都为1(避免出现被0除)
        logging.error(u'文件读取错误，文件名=%s,错误信息=%s' % (filename,str(inst)))
    return records


def test():
    return read_min1('y1401') #+ read_min1('p1401') + read_min1('OI1401')

##转换
from adapter.sqlite.minute import minute
s2011 = minute(2011)
s2012 = minute(2012)
s2013 = minute(2013)
s2014 = minute(2014)
stest = minute(2099)

##整体导入
def transfer1_min(smin,cname,extractor=extractor_m1,tfrom=0,tto=99999999):
    smin.create_table(cname)
    smin.remove_by_date(cname,dfrom=tfrom,dto=tto)
    rs = read_min1(cname,extractor=extractor,tfrom=tfrom,tto=tto)
    smin.insert_rows(cname,rs)

transfer1_min_10 = fcustom(transfer1_min,extractor = extractor_m1_10)
transfer1_min_100 = fcustom(transfer1_min,extractor = extractor_m1_100)

def transfer_min(smin,contracts,ftransfer1=transfer1_min,tfrom=0,tto=99999999):
    smin.open_connect()
    for contract in contracts:
        ftransfer1(smin,contract,tfrom=tfrom,tto=tto)
    smin.close_connect()

transfer_min_10 = fcustom(transfer_min,ftransfer1 = transfer1_min_10)
transfer_min_100 = fcustom(transfer_min,ftransfer1 = transfer1_min_100)

##update导入
def update1_min(smin,cname,extractor=extractor_m1):
    smin.create_table(cname)
    dlast = smin.last_date(cname)
    rs = read_min1(cname,extractor=extractor,tfrom=dlast+1)
    print(cname,dlast,len(rs))
    smin.insert_rows(cname,rs)

update1_min_10 = fcustom(update1_min,extractor = extractor_m1_10)
update1_min_100 = fcustom(update1_min,extractor = extractor_m1_100)

def update_min(smin,contracts,fupdate1=update1_min):
    smin.open_connect()
    for contract in contracts:
        fupdate1(smin,contract)
    smin.close_connect()

update_min_10 = fcustom(update_min,fupdate1 = update1_min_10)
update_min_100 = fcustom(update_min,fupdate1 = update1_min_100)

'''
#使用举例
A.
In [1]: import adapter.tradeblazer as tb

In [2]: tb.s2013.open_connect()

In [3]: tb.update1_min(tb.s2013,'y1309')
0 43940

In [4]: tb.update1_min(tb.s2013,'y1309')
20130812 0

In [5]: tb.s2013.close_connect()

B.
In [1]: import adapter.tradeblazer as tb

In [2]: tb.update_min(tb.s2013,['y1309'])
0 43940

In [3]: tb.update_min(tb.s2013,['y1309'])
20130812 0

In [4]: tb.update_min(tb.s2013,['m1309','OI1309'])
0 36392
0 25544

In [5]: tb.update_min(tb.s2013,['m1309','OI1309'])
20130524 0
20130812 0

C.
In [1]: import adapter.tradeblazer as tb

In [2]: tb.transfer_min(tb.stest,['y1309'])

In [3]: tb.transfer_min(tb.stest,['y1309'])

In [4]: tb.transfer_min(tb.stest,['y1309','c1309'])


'''

'''
数据倒入
In [1]: import adapter.tradeblazer as tb
In [2]: tb.transfer_2011()
In [3]: tb.transfer_2012()
In [4]: tb.transfer_2013()
In [5]: tb.transfer_2014()
'''
def transfer_2011():
    update_min_10(s2011,t2011_b)

def transfer_2012():
    update_min(s2012,t2012_a)
    update_min_10(s2012,t2012_b)
    update_min_100(s2012,t2012_c)

def transfer_2013():
    update_min(s2013,t2013_a)
    update_min_10(s2013,t2013_b)
    update_min_100(s2013,t2013_c)

def transfer_2014():
    update_min(s2014,t2014_a)
    update_min_10(s2014,t2014_b)
    update_min_100(s2014,t2014_c)


t2011_b = ('IF1108','IF1109','IF1110','IF1111','IF1112')
t2012_a = ('a1205','a1209',
           'ag1212',
           'al1208','al1209','al1210','al1211','al1212',
           'c1209',
           'CF1209',
           'cu1208','cu1209','cu1210','cu1211','cu1212',
           'ER1209',
           'j1209',
           'l1209',
           'm1209',
           'ME1209',
           'p1209',
           'rb1209','rb1210',
           'RO1209',
           'ru1209',
           'SR1205','SR1209',
           'TA1209',
           'v1209',
           'WS1205','WS1209',
           'y1209',
           'zn1208','zn1209','zn1210','zn1211','zn1212',
           )

t2012_b = ('IF1201','IF1202','IF1203','IF1204','IF1205','IF1206','IF1207','IF1208','IF1209','IF1210','IF1211','IF1212')
t2012_c = ('au1212',)

t2013_a = ('a1301','a1305','a1309',
           'ag1306','ag1312',
           'al1301','al1302','al1303','al1304','al1305','al1306','al1307','al1308','al1309','al1310','al1311','al1312',
           'c1301','c1305','c1309',
           'CF1301','CF1305','CF1309',
           'cu1301','cu1302','cu1303','cu1304','cu1305','cu1306','cu1307','cu1308','cu1309','cu1310','cu1311','cu1312',
           'ER1301','ER1305',
           'FG1305','FG1309',
           'j1301','j1305','j1309',
           'l1301','l1305','l1309',
           'm1301','m1309','m1309',
           'ME1301','ME1305','ME1309',
           'OI1309',
           'p1301','p1305','p1309',
           'PM1301','PM1305','PM1309',
           'rb1301','rb1305','rb1310',
           'RI1309',
           'RM1305','RM1309',
           'RO1301','RI1305','RO1309',
           'RS1309',
           'ru1301','ru1305','ru1309',
           'SR1301','SR1305','SR1309',
           'TA1301','TA1305','TA1309',
           'v1301','v1305','v1309',
           'WS1301','WS1305',
           'y1301','y1305','y1309',
           'zn1301','zn1302','zn1303','zn1304','zn1305','zn1306','zn1307','zn1308','zn1309','zn1310','zn1311','zn1312',
           )

t2013_b = ('IF1301','IF1302','IF1303','IF1304','IF1305','IF1306','IF1307','IF1308','IF1309','IF1310','IF1311','IF1312')
t2013_c = ('au1306','au1312',)

t2014_a = ('a1401','a1405','a1409',
           'ag1406','ag1412',
           #'al1401','al1402','al1403','al1404','al1405','al1406','al1407','al1408','al1409','al1410','al1411','al1412',
           'c1401','c1405','c1409',
           'CF1401','CF1405','CF1409',
           #'cu1401','cu1402','cu1403','cu1404','cu1405','cu1406','cu1407','cu1408','cu1409','cu1410','cu1411','cu1412',
           'ER1401','ER1405',
           'FG1405','FG1409',
           'j1401','j1405','j1409',
           'l1401','l1405','l1409',
           'm1401','m1409','m1409',
           'ME1401','ME1405','ME1409',
           'OI1409',
           'p1401','p1405','p1409',
           'PM1401','PM1405','PM1409',
           'rb1401','rb1405','rb1410',
           'RI1409',
           'RM1405','RM1409',
           'RO1401','RI1405','RO1409',
           'RS1409',
           'ru1401','ru1405','ru1409',
           'SR1401','SR1405','SR1409',
           'TA1401','TA1405','TA1409',
           'v1401','v1405','v1409',
           'WS1401','WS1405',
           'y1401','y1405','y1409',
           #'zn1401','zn1402','zn1403','zn1404','zn1405','zn1406','zn1407','zn1408','zn1409','zn1410','zn1411','zn1412',
           )

t2014_b = ('IF1401','IF1402','IF1403','IF1404','IF1405','IF1406','IF1407','IF1408','IF1409','IF1410','IF1411','IF1412')
t2014_c = ('au1406','au1412',)

