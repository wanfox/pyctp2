#-*- coding: utf-8 -*-

import sys
import functools

import logging

XBASE = 100 #用于一般化的除数基数

MY_FORMAT = '%(name)s:%(funcName)s:%(lineno)d:%(asctime)s %(levelname)s %(message)s'
CONSOLE_FORMAT = '**%(message)s'


####日志函数
#设定日志
def config_logging(filename,level=logging.DEBUG,format=MY_FORMAT,to_console=True,console_level=logging.INFO):
    logging.basicConfig(filename=filename,level=level,format=format)
    if to_console:
        add_log2console(console_level)

#将指定级别的日志同时输出到控制台
def add_log2console(level = logging.INFO):
    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = logging.Formatter(CONSOLE_FORMAT)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)



####根据日期得到星期数
def date2week(iday):
    #http://blog.csdn.net/hawkfeifei/article/details/4337181
    year = iday//10000
    month = iday//100%100
    day = iday%100
    if month <= 2:
        month += 12
        year -= 1
    return (day+2*month+3*(month+1)//5+year+year//4-year//100+year//400)%7 + 1  #转化为1-7


####函数参数定制
def fcustom(func,**kwargs):
    ''' 根据kwargs设置func的偏函数,并将此偏函数的名字设定为源函数名+所固定的关键字参数名
    '''
    pf = functools.partial(func,**kwargs)
    pf.paras = ','.join(['%s=%s' % item for item in pf.keywords.items()])
    pf.__name__ = '%s:%s' % (func.__name__,pf.paras)
    return pf

