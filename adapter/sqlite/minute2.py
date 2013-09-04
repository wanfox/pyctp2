# -*- coding: utf-8 -*-
'''
    minute的直接RAW SQL的实现
'''

import sqlite3

from core.base import BaseObject,XMIN

PATH_SQL_MIN1 = 'data/sqlite/min1/'

SQL_MIN_TABLE = '''create table if not exists 
    %(name)s(
        idate integer,
        imin integer,
        iopen integer,
        iclose integer,
        ihigh integer,
        ilow integer,
        ivolume integer,
        iholding integer,
        itype bool, --h/l occurred order
        primary key(idate,imin) --分钟数据每个合约一张表,故主键为日期+分钟
    )
    '''

SQL_MIN_DATA_SOURCE_CLAUSE = '''select '%(name)s' as cname,idate,imin,iopen,iclose,ihigh,ilow,ivolume,iholding,itype
        from %(name)s
        where idate >= %(dfrom)d and idate<%(dto)d
    '''
SQL_MIN_DATA_ORDER_CLAUSE = '''
        order by idate,imin
    '''
SQL_INSERT_MIN = '''insert into %s(idate,imin,iopen,iclose,ihigh,ilow,ivolume,iholding,itype) 
        values (:idate,:imin,:iopen,:iclose,:ihigh,:ilow,:ivolume,:iholding,:itype)
    '''
SQL_UPDATE_ITYPE_FOR_MIN = '''update %s set itype=:itype where idate = :idate and imin = :imin'''


class minute2(object):
    def __init__(self,year,path=PATH_SQL_MIN1):
        self.year = year
        self.path = path
        self.dbname = PATH_SQL_MIN1 + str(year)
        self.connect = None

    def open_connect(self):
        if self.connect == None:
            self.connect = sqlite3.connect(self.dbname)

    def close_connect(self):
        self.connect.close()
        self.connect = None

    def create_table(self,cname):
        cursor = self.connect.cursor()
        cursor.execute(SQL_MIN_TABLE % {'name':cname})
        self.connect.commit()
        cursor.close()

    def query_by_date(self,cnames,dfrom=0,dto=99999999):
        '''
            cnames: 合约列表
            返回XMIN列表
        '''
        assert len(cnames) > 0
        self.connect.row_factory = self._row_factory
        ss = [ SQL_MIN_DATA_SOURCE_CLAUSE % BaseObject(name=cname,dfrom=dfrom,dto=dto).mydict() for cname in cnames ]
        source = ' union\n'.join(ss) + SQL_MIN_DATA_ORDER_CLAUSE
        cursor = self.connect.cursor()
        cursor.execute(source)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def insert_rows(self,cname,rows):
        '''
            rows为[BaseObject,]列表
        '''
        cursor = self.connect.cursor()
        cursor.executemany(SQL_INSERT_MIN % (cname,),[row.mydict() for row in rows])
        self.connect.commit()
        cursor.close()

    def update_rows(self,cname,rows):
        '''
            更新itype, 其它字段不需要更新
            其中rows为列表
        '''
        cursor = self.connect.cursor()
        #cursor.executemany(SQL_UPDATE_ITYPE_FOR_MIN % (cname,),[row.mydict() for row in rows])
        cursor.executemany(SQL_UPDATE_ITYPE_FOR_MIN % (cname,),[row.mydict_simple() for row in rows])
        self.connect.commit()
        cursor.close()

    def remove_by_date(self,cname,dfrom=0,dto=99999999):
        cursor = self.connect.cursor()
        cursor.execute('delete from %s where idate >= ? and idate < ?' % (cname,),(dfrom,dto))
        self.connect.commit()
        cursor.close()


    def last_date(self,cname):
        cursor = self.connect.cursor()
        cursor.execute('select max(idate) from %s' % (cname,))
        ilast = cursor.fetchone()[0]
        cursor.close()
        if ilast != None:
            return ilast
        return 0
    
    @staticmethod
    def _row_factory(cursor,row):
        cd = cursor.description
        #assert cd[0][0] == 'cname', 'desc=%s' % (cd[0][0],)
        #return BaseObject(cname=row[0],idate=row[1],imin=row[2],iopen=row[3],iclose=row[4],ihigh=row[5],ilow=row[6],ivolume=row[7],iholding=row[8],itype=row[9])
        return XMIN(cname=row[0],idate=row[1],imin=row[2],iopen=row[3],iclose=row[4],ihigh=row[5],ilow=row[6],ivolume=row[7],iholding=row[8],itype=row[9])





'''
测试:
In [1]: import adapter.sqlite.minute2 as minute2

In [2]: import adapter.tradeblazer as tb

In [3]: sm1 = minute2.minute2(2019)

In [4]: sm1.open_connect()

In [5]: rs = tb.test()

In [6]: sm1.create_table('m1401')

In [7]: sm1.insert_rows('m1401',rs)

In [8]: sm1.close_connect()

In [9]: sm1 = minute2.minute2(2019)

In [10]: sm1.open_connect()

In [11]: rows = sm1.query_by_date(['m1401'])

In [12]: len(rows)
Out[12]: 25210

In [13]: rows[0]
Out[13]: m1401:20130118-903 8760-8756-8760-8756 8-8 0

In [14]: rows[-1]
Out[14]: m1401:20130802-1459 7042-7034-7046-7030 16814-740434 0

In [15]: sm1.remove_by_idate('m1401',dfrom=20130505)

In [16]: sm1.remove_by_idate('m1401',dfrom=20130501)

In [17]: rows[0].itype=1

In [18]: rows[2].itype=3

In [19]: sm1.update_rows('m1401',rows[:3])

'''
