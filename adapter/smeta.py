# -*- coding: utf-8 -*-

'''
数据的元信息. sqlite实现
1. 品种信息
2. 合约信息
'''

from core.utils import fcustom


import adapter.sqlite as s3

def connect_meta_db():
    dbname = s3.PATH_SQL_MIN1 + 'SMETA'
    return sqlite3.connect(dbname)


SQL_COMMODITY_TABLE = '''create table if not exists commodity
    (   name text, --中文名字
        sname text primary key, --名字缩写
        market text, --市场， 上期/中金/郑商/大商
        unit integer, --最小交易单位, IF为2, 黄金为1(0.01)
        factor integer, --缩放因子, IF为10, 黄金为100,通常为1
        orignal_date integer, --创建时间
        terminate_date integer, --结束时间
        multiplier integer, --合约乘数, IF为300
        description text, --描述
    )
    '''

SQL_CONTRACT_TABLE = '''create table if not exists contract
    (   name text primary key, --合约名称
        typename text,  --commodity的名称
        unit integer, --最小交易单位, IF为2, 黄金为1(0.01)
        factor integer, --缩放因子, IF为10, 黄金为100,通常为1
        multiplier integer, --合约乘数, IF为300
        start_date integer, --合约开始时间
        end_date integer,   --合约结束时间
        last_date integer,  --最后交易日, 信息覆盖时间cover_day
        mature_date integer, --成熟日期, 从满足成熟标准的第一天的次日开始. 但这里记录的是成熟的第一天。所以使用时必须为>
        exit_date integer,  --衰弱日期, 连续3天交易量小于成熟标准, 记录的是第3天。所以使用时必须为>
        smature integer, --成熟标准, 成交量/(最高-最低)的倍数
        description text, --描述
    '''

create_commodity_table_if_not_exists = fcustom(s3.create_table_if_not_exists,sql_template = SQL_COMMODITY_TABLE)

create_contract_table_if_not_exists = fcustom(s3.create_table_if_not_exists,sql_template = SQL_CONTRACT_TABLE)

'''
    查询相关
'''

SQL_COMMODITY_QUERY = '''select name,sname,unit,factor,orignal_date,terminate_date,multiplier,description from commodity'''
SQL_CONTRACT_QUERY = '''select name,typename,unit,factor,multiplier,start_date,end_date,last_date,mature_date,exit_date,smature,description from contract'''

def query_meta(conn,row_factory,sql_query):
    conn.row_factory = row_factory
    cursor = conn.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    cursor.close()
    return rows
    
def commodity_row_factory(cursor,row):
    cd = cursor.description
    return BaseObject(name=row[0],sname=row[1],unit=row[2],factor=row[3],orignal_date=row[4],terminate_date=row[5],multiplier=row[6],description=row[7])

def contract_row_factory(cursor,row):
    cd = cursor.description
    return BaseObject(name=row[0],typename=row[1],unit=row[2],factor=row[3],
                      multiplier=row[4],start_date=row[5],end_date=row[6],last_date=row[7],
                      mature_date=row[8],exit_date=row[9],smature=row[10],description=row[11]
                )


'''
insert
'''
def insert_commodity_rows(conn,rows):
    cursor = conn.cursor()
    cursor.executemany('insert into commodity values (:name,:sname,:market,:unit,:factor,:orignal_date,:terminate_date,:multiplier,:description)',[row.mydict() for row in rows])
    conn.commit()
    cursor.close()

def insert_contract_rows(conn,rows):
    cursor = conn.cursor()
    cursor.executemany('insert into contract values (:name,:typename,:unit,:factor,:multiplier,:start_date,:end_date,:last_date,:mature_date,:exit_date,:smature,:description)',[row.mydict() for row in rows])
    conn.commit()
    cursor.close()

'''
update
'''
def update_commodity_rows(conn,rows):
    cursor = conn.cursor()
    cursor.executemany('''update commodity set sname=:sname,market=:market,unit=:unit,factor=:factor,orignal_date=:orignal_date,terminate_date=:terminate_date,
        multiplier=:multiplier,description=:description where name=:name''',[row.mydict() for row in rows])
    conn.commit()
    cursor.close()

def update_commodity_rows(conn,rows):
    cursor = conn.cursor()
    cursor.executemany('''update contract set typename=:typename,unit=:unit,factor=:factor,start_date=:start_date,end_date=:end_date,last_date=:last_date,
        mature_date=:mature_date,exit_date=:exit_date,smature=:smature,multiplier=:multiplier,description=:description where name=:name''',
        [row.mydict() for row in rows])
    conn.commit()
    cursor.close()

'''
delete
'''
def remove_commodity_rows(conn,tbname,rows):
    cursor = conn.cursor()
    cursor.executemany('delete from commodity where name=:name',[row.mydict() for row in rows])
    conn.commit()
    cursor.close()

def remove_contract_rows(conn,tbname,rows):
    cursor = conn.cursor()
    cursor.executemany('delete from contract where name=:name',[row.mydict() for row in rows])
    conn.commit()
    cursor.close()

