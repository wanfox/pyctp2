# -*- coding: utf-8 -*-

'''
    minute的sobject实现
    用于实时保存数据?
    结合utils.merge,可实现类似于minute2的query_by_date
'''

import sqlite3

from core.base import BaseObject,XMIN
from adapter.sqlite.cmapper import (sobject,column,CTYPE)

PATH_SQL_MIN1 = 'data/sqlite/min1/'

class minute(sobject):
    def __init__(self,year,cname):
        columns = [column('idate',CTYPE.Integer,'日期'),
                   column('imin', CTYPE.Integer,'分钟'),
                   column('iopen', CTYPE.Integer,'开盘'),
                   column('iclose', CTYPE.Integer,'收盘'),
                   column('ihigh', CTYPE.Integer,'最高'),
                   column('ilow', CTYPE.Integer,'最低'),
                   column('ivolume', CTYPE.Integer,'成交量'),
                   column('iholding', CTYPE.Integer,'持仓'),
                   column('itype', CTYPE.Integer,'高低出现顺序'),
                ]
        sobject.__init__(self,PATH_SQL_MIN1 + str(year),cname,columns,primary_key=('idate','imin'))


    @staticmethod
    def open_connect_by_year(year):
        #print(PATH_SQL_MIN1 + str(year))
        return sqlite3.connect(PATH_SQL_MIN1 + str(year))

    def query_by_date(self,dfrom=0,dto=99999999):
        '''
            返回XMIN列表
        '''
        return self.query_by_range(BaseObject(column=self.IDATE,vfrom=dfrom,vto=dto))

    def remove_by_date(self,dfrom=0,dto=99999999):
        return self.remove_by_range(BaseObject(column=self.IDATE,vfrom=dfrom,vto=dto))

    def last_date(self):
        cursor = self.connect.cursor()
        cursor.execute('select max(idate) as mdate from %s' % (self.sname,))
        ilast = cursor.fetchone().mdate
        cursor.close()
        if ilast != None:
            return ilast
        return 0
    
    def _row_factory(self,cursor,row):
        if len(row) >= 8:
            return XMIN(cname=self.sname,idate=row[0],imin=row[1],iopen=row[2],iclose=row[3],ihigh=row[4],ilow=row[5],ivolume=row[6],iholding=row[7],itype=row[8])
        else:
            return sobject._row_factory(self,cursor,row)



