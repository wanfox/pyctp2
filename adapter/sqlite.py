# -*- coding: utf-8 -*-

import sqlite3

from core.base import BaseObject,XMIN
from core.utils import fcustom

PATH_SQL_MIN1 = 'data/sqlite/min1/'


'''
    connect创建相关
'''
def get_min1_dbname(year):
    return PATH_SQL_MIN1 + str(year)

def connect_min1_db(year):
    dbname = get_min1_dbname(year)
    return sqlite3.connect(dbname)

'''
    建表相关
'''
#建表
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

def create_table_if_not_exists(connect,tbname,sql_template):
    ''' con = sqlite3.connect('XXX')
        create_table_if_not_exists(con,tbname,SQL_MIN_TABLE)
    '''
    cursor = connect.cursor()
    cursor.execute(sql_template % {'name':tbname})
    connect.commit()
    cursor.close()
    
create_min_table_if_not_exists = fcustom(create_table_if_not_exists,sql_template = SQL_MIN_TABLE)

'''
    查询/删除/UPDATE相关
'''
#查询, 需要union
SQL_MIN_DATA_SOURCE_CLAUSE = '''select '%(name)s' as cname,idate as idate,imin,iopen,iclose,ihigh,ilow,ivolume,iholding,itype
        from %(name)s
        where idate >= %(dfrom)d and idate<%(dto)d
    '''
SQL_MIN_DATA_ORDER_CLAUSE = '''
        order by idate,imin
    '''


def query(conn,cnames,row_factory,source_clause,order_clause,dfrom=0,dto=99999999):
    '''
        conn:数据库连接
        cnames: 合约列表
        source_clause: 查询主语句
        order_clause: 查询排序语句
        dfrom: 开始日期 >=
        dto: 结束日期 <, 即[dfrom,dto)
    '''
    assert len(cnames) > 0
    conn.row_factory = row_factory
    ss = [ source_clause % BaseObject(name=cname,dfrom=dfrom,dto=dto).mydict() for cname in cnames ]
    source = ' union\n'.join(ss) + order_clause
    cursor = conn.cursor()
    cursor.execute(source)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def min_factory(cursor,row):
    cd = cursor.description
    #assert cd[0][0] == 'cname', 'desc=%s' % (cd[0][0],)
    #return BaseObject(cname=row[0],idate=row[1],imin=row[2],iopen=row[3],iclose=row[4],ihigh=row[5],ilow=row[6],ivolume=row[7],iholding=row[8],itype=row[9])
    return XMIN(cname=row[0],idate=row[1],imin=row[2],iopen=row[3],iclose=row[4],ihigh=row[5],ilow=row[6],ivolume=row[7],iholding=row[8],itype=row[9])

query_min = fcustom(query,row_factory=min_factory,source_clause = SQL_MIN_DATA_SOURCE_CLAUSE,order_clause = SQL_MIN_DATA_ORDER_CLAUSE)


def remove_rows(conn,tbname,tfrom=0,tto=99999999):
    '''
        按日删除, 没必要到分钟
    '''
    #print('remove rows')
    cursor = conn.cursor()
    cursor.execute('delete from %s where idate >= ? and idate < ?' % (tbname,),(tfrom,tto))
    conn.commit()
    cursor.close()

def last_date(conn,tbname):
    cursor = conn.cursor()
    cursor.execute('select max(idate) from %s' % (tbname,))
    ilast = cursor.fetchone()[0]
    cursor.close()
    if ilast != None:
        return ilast
    return 0

def insert_min_rows(conn,tbname,rows):
    cursor = conn.cursor()
    #cursor.executemany('insert into %s values (?,?,?,?,?,?,?,?,?)' % (tbname,),rows)
    cursor.executemany('insert into %s values (:idate,:imin,:iopen,:iclose,:ihigh,:ilow,:ivolume,:iholding,:itype)' % (tbname,),[row.mydict() for row in rows])
    #for row in rows:
        #cursor.execute('insert into %s values (?,?,?,?,?,?,?,?,?)' % (tbname,),(row.idate,row.itime,row.iopen,row.iclose,row.ihigh,row.ilow,row.ivol,row.iholding,False))
        #cursor.execute('insert into %s values (:idate,:imin,:iopen,:iclose,:ihigh,:ilow,:ivolume,:iholding,:itype)' % (tbname,),row.mydict())
    conn.commit()
    cursor.close()

def insert_min_rows2(conn,tbname,rows):
    '''
        测试用相对方式,即收盘/最高/最低 都用与开盘价的差值来存储的 空间节省
        测试结果: 空间节省15%左右, 但压缩后空间反而增大15%
    '''
    cursor = conn.cursor()
    #cursor.executemany('insert into %s values (?,?,?,?,?,?,?,?,?)' % (tbname,),rows)
    for row in rows:
        cursor.execute('insert into %s values (?,?,?,?,?,?,?,?,?)' % (tbname,),(row.idate,row.itime,row.iopen,row.iclose-row.iopen,row.ihigh-row.iopen,row.ilow-row.iopen,row.ivol,row.iholding,False))
    conn.commit()
    cursor.close()


'''
    使用举例
'''
import sqlite3
def create_test_db():
    conn = sqlite3.connect(PATH_SQL_MIN1 + 'XX')
    #create_table_if_not_exists(conn,'y1401',SQL_MIN_TABLE)
    create_min_table_if_not_exists(conn,'y1401')
    create_min_table_if_not_exists(conn,'p1401')
    create_min_table_if_not_exists(conn,'OI1401')    
    conn.close()
    conn = sqlite3.connect(PATH_SQL_MIN1 + 'YY')
    create_min_table_if_not_exists(conn,'y1401')
    create_min_table_if_not_exists(conn,'p1401')
    create_min_table_if_not_exists(conn,'OI1401')    
    conn.close()

'''
测试1:
    import adapter.sqlite as s3
    s3.create_test_db()
    import adapter.tradeblazer as tb
    rs = tb.test()
    s3.insert_min_rows('XX','y1401',rs)
    s3.insert_min_rows2('YY','y1401',rs)
    #测试结果, 实际体积 XX > YY(15%), 但压缩后体积 XX<YY(15%), 没有实际意义!!

测试2: #是否加primary key(idate,imin),  ##添加主键后, 体积增大50%; 压缩后体积增大30%
    In [1]: import sqlite3
    In [2]: import adapter.sqlite as s3
    In [3]: import adapter.tradeblazer as tb
    In [4]: s3.create_test_db()
    In [5]: conn = sqlite3.connect(s3.PATH_SQL_MIN1 + 'XX')
    In [6]: rs = tb.read_min1('y1401')
    In [7]: s3.insert_min_rows(conn,'y1401',rs)
    In [8]: rs = tb.read_min1('p1401')
    In [9]: s3.insert_min_rows(conn,'p1401',rs)
    In [10]: rs = tb.read_min1('OI1401')
    In [11]: s3.insert_min_rows(conn,'OI1401',rs)
    In [12]: conn.close()

'''
