# -*- coding: utf-8 -*-

path_sql_min1 = 'sqlite/min1/'

sql_min_table = '''create table %(name)s(
        idate integer,
        imin integer,
        iopen integer,
        iclose integer,
        ihigh integer,
        ilow integer,
        ivolume integer,
        iholding integer,
        itype bool --h/l occurred order
    )
    '''

def create_table(connect,tbname,sql_template):
    ''' con = sqlite3.connect('XXX')
        create_table(con,tbname,sql_min_table)
    '''
    cursor = connect.cursor()
    cursor.execute(sql_min_table % {'name':tbname})
    connect.commit()
    cursor.close()
    

def insert_min_rows(dbname,tbname,rows):
    conn = sqlite3.connect('data/sqlite/min1/%s' % (dbname,))
    cursor = conn.cursor()
    cursor.executemany('insert into %s values (?,?,?,?,?,?,?,?,?)' % (tbname,),rows)
    conn.commit()
    cursor.close()
    conn.close()

def test_volume():
    r = [20130201,915,25000,25100,25500,24900,100000,200000,False]
    rs = [r] * 50000
    r2 = [20130201,915,25000,100,400,-100,100000,200000,False]
    r2s = [r2] * 50000
    insert_min_rows('XX','IF0000',rs)
    insert_min_rows('YY','IF0000',r2s)

import sqlite3
def create_test_db():
    conn = sqlite3.connect('data/sqlite/min1/XX')
    create_table(conn,'IF0000',sql_min_table)
    conn.close()
    conn = sqlite3.connect('data/sqlite/min1/YY')
    create_table(conn,'IF0000',sql_min_table)
    conn.close()

'''
测试:
    import adapter.sqlite as s3
    conn = sqlite3.connect('data/sqlite/min1/XX')
    s3.create_table(conn,'IF0000',s3.sql_min_table)
    conn.close()
    conn = sqlite3.connect('data/sqlite/min1/YY')
    s3.create_table(conn,'IF0000',s3.sql_min_table)
    conn.close()

    
'''
