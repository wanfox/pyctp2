# -*- coding: utf-8 -*-

import core.base
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
    record.date = int(items[0].replace('/',''))
    record.time = int(items[1].replace(':',''))
    record.open = int(items[2])
    record.high = int(items[3])
    record.low = int(items[4])
    record.close = int(items[5])
    record.vol = int(items[6]) 
    record.holding = int(items[7]) 
    return record
    
def extractor_m1_10(line):
    '''
        仅用于IF或带一位小数价位的合约
    '''
    items = re.split(',| ',line)
    record = BaseObject()
    record.date = int(items[0].replace('/',''))
    record.time = int(items[1].replace(':',''))
    record.open = int(float(items[2])*10 + 0.1)
    record.high = int(float(items[3])*10 + 0.1)
    record.low = int(float(items[4])*10 + 0.1)
    record.close = int(float(items[5])*10 + 0.1)
    record.vol = int(float(items[6]) + 0.1)
    record.holding = int(float(items[7]) + 0.1)
    return record

def read_min1(cname,extractor=extractor_m1):
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
                records.append(extractor_m1(line))
    except Exception as inst:#读不到数据,默认都为1(避免出现被0除)
        logging.error(u'文件读取错误，文件名=%s,错误信息=%s' % (filename,str(inst)))
    return records


def test():
    return read_min1('y1401')

