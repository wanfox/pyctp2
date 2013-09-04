# -*- coding: utf-8 -*-

import time
import logging
import re


from core.base import (BaseObject,)
from core.utils import (fcustom)

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
#合约列表
from adapter.contracts import (t2011_b,t2012_a,t2012_b,t2012_c,t2013_a,t2013_b,t2013_c,t2014_a,t2014_b,t2014_c)

'''
minute2版本
'''
from adapter.sqlite.minute2 import minute2
s2011 = minute2(2011)
s2012 = minute2(2012)
s2013 = minute2(2013)
s2014 = minute2(2014)
stest = minute2(2099)

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

def transfer_2():
    tbegin = time.time()
    transfer_2011()
    transfer_2012()
    transfer_2013()
    transfer_2014()    
    print('transfer use time:%s seconds:' % (time.time() - tbegin,))

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

'''
使用常规minute类
'''
from adapter.sqlite.minute import minute
##整体导入
def transfer1_min_s(connect,year,cname,extractor=extractor_m1,tfrom=0,tto=99999999):
    cmin = minute(year,cname)
    cmin.put_connect(connect)
    cmin.create_table_if_not_exists()
    cmin.remove_by_date(dfrom=tfrom,dto=tto)
    rs = read_min1(cname,extractor=extractor,tfrom=tfrom,tto=tto)
    cmin.insert(rs)
    cmin.release_connect()

transfer1_min_s_10 = fcustom(transfer1_min_s,extractor = extractor_m1_10)
transfer1_min_s_100 = fcustom(transfer1_min_s,extractor = extractor_m1_100)

def transfer_min_s(year,contracts,ftransfer1=transfer1_min_s,tfrom=0,tto=99999999):
    connect = minute.open_connect_by_year(year)
    for contract in contracts:
        ftransfer1(connect,year,contract,tfrom=tfrom,tto=tto)
    connect.close()

transfer_min_s_10 = fcustom(transfer_min_s,ftransfer1 = transfer1_min_s_10)
transfer_min_s_100 = fcustom(transfer_min_s,ftransfer1 = transfer1_min_s_100)

##update导入
def update1_min_s(connect,year,cname,extractor=extractor_m1):
    cmin = minute(year,cname)
    #cmin.put_connect(connect)
    cmin.open_connect()
    cmin.create_table_if_not_exists()
    dlast = cmin.last_date()
    rs = read_min1(cname,extractor=extractor,tfrom=dlast+1)
    print(cname,dlast,len(rs))
    cmin.insert(rs)
    cmin.release_connect()


update1_min_s_10 = fcustom(update1_min_s,extractor = extractor_m1_10)
update1_min_s_100 = fcustom(update1_min_s,extractor = extractor_m1_100)

def update_min_s(year,contracts,fupdate1=update1_min_s):
    connect = minute.open_connect_by_year(year)
    for contract in contracts:
        fupdate1(connect,year,contract)
    connect.close()

update_min_s_10 = fcustom(update_min_s,fupdate1 = update1_min_s_10)
update_min_s_100 = fcustom(update_min_s,fupdate1 = update1_min_s_100)

def transfer_s_2011():
    update_min_s_10(2011,t2011_b)

def transfer_s_2012():
    update_min_s(2012,t2012_a)
    update_min_s_10(2012,t2012_b)
    update_min_s_100(2012,t2012_c)

def transfer_s_2013():
    update_min_s(2013,t2013_a)
    update_min_s_10(2013,t2013_b)
    update_min_s_100(2013,t2013_c)

def transfer_s_2014():
    update_min_s(2014,t2014_a)
    update_min_s_10(2014,t2014_b)
    update_min_s_100(2014,t2014_c)


def transfer_s():
    tbegin = time.time()
    transfer_s_2011()
    transfer_s_2012()
    transfer_s_2013()
    transfer_s_2014()    
    print('transfer_s use time:%s seconds:' % (time.time() - tbegin,))
