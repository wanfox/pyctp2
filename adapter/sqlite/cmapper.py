# -*- coding:utf-8 -*-

import sqlite3
from core.base import (BaseObject,)

EMPTY_OBJECT = BaseObject()

class CTYPE(object):
    Integer = BaseObject(stype='integer',default=0)
    String = BaseObject(stype='text',default="''")
    Bool = BaseObject(stype='integer',default=0)

class column(object):
    itype = type(0)
    stype = type('')
    def __init__(self,name,ctype,description='',order_by = 0):
        self.name = name
        self.ctype = ctype
        self.description = description
        self.order_by = order_by    #order_by>0时,按序作为排序键
    
    def tvalue(self,v): #根据value获得用于数据操作语句的形式
        if self.ctype == CTYPE.Integer or self.ctype == CTYPE.Bool:
            return v
        elif self.ctype == CTYPE.String:
            return "'%s'" % (v,)
        else:
            return v

    def check_type(self,v):
        if self.ctype == CTYPE.Integer or self.ctype == CTYPE.Bool:
            return type(v) == self.itype
        elif self.ctype == CTYPE.String:
            return type(v) == self.stype
        else:
            return False


class sobject(object):
    def __init__(self,dbname,sname,columns,primary_key=(),indexes=()):
        '''
            primary_key为列名的tuple
            index为 (列名1,列名2..)组成的元组
        '''
        self.dbname = dbname
        self.sname = sname
        self.columns = columns
        self.column_map = dict([(column.name,column) for column in columns])
        self.__dict__.update(dict([(column.name.upper(),column) for column in columns]))
        self.primary_key = primary_key
        self.indexes = indexes
        self.order_bys = self._check_order_by()
        self.select_columns = ','.join([c.name for c in self.columns])        
        self.sql_create = self._create_sql_create()
        self.sql_query = self._create_sql_query()
        self.sql_insert = self._create_sql_insert()
        self.sql_update = self._create_sql_update()
        self.sql_remove = self._create_sql_remove()
        self.row_factory = self._row_factory
        self.connect = None

    def open_connect(self):
        '''
            打开连接
        '''
        if self.connect == None:
            self.connect = sqlite3.connect(self.dbname)
            self.connect.row_factory = self.row_factory

    def close_connect(self):
        '''
            关闭连接
        '''
        self.connect.close()
        self.connect = None

    def put_connect(self,connect):
        '''
            置入连接
        '''
        self.connect = connect
        self.connect.row_factory = self.row_factory
      
    def release_connect(self):
        '''
            释放连接, 置入的逆操作
        '''
        self.connect = None

    def create_table_if_not_exists(self):
        cursor = self.connect.cursor()
        
        #create table
        cursor.execute(self.sql_create)
        #creta index
        for sindex in self.indexes:
            #print(self._create_sql_index(sindex))
            cursor.execute(self._create_sql_index(sindex))
        
        self.connect.commit()
        cursor.close()

    def query(self):#返回所有记录
        return self._query(self.sql_query)

    def query_by_value(self,vcolumn):
        '''
            根据值查询
            vcolumn包含属性: name,value
        '''
        ss = self._get_select_clause() + self._get_condition_clause_by_value(vcolumn)
        return self._query(ss)

    def query_by_range(self,rcolumn):
        '''
            根据范围查询
            rcolumn包含属性name,vfrom,vto
        '''
        ss = self._get_select_clause() + self._get_condition_clause_by_range(rcolumn)
        return self._query(ss)

    def query_by_range2(self,rcolumn1,rcolumn2):
        '''
            根据两个范围查询
            rcolumn1,rcolumn2均包含属性name,vfrom,vto
        '''
        ss = self._get_select_clause() + self._get_condition_clause_by_range2(rcolumn1,rcolumn2)
        return self._query(ss)

    def query_by_value_range(self,vcolumn,rcolumn):
        '''
            根据两个范围查询
            rcolumn1,rcolumn2均包含属性name,vfrom,vto
        '''
        ss = self._get_select_clause() + self._get_condition_clause_by_value_range(vcolumn,rcolumn)
        return self._query(ss)

    def query_by_raw(self,sql_condition,param=EMPTY_OBJECT):
        '''
            condition: 如 'itype=:itype'
            param: BaseObject,其__dict__为:{'type':1}
            原始的sql方式
        '''
        condition_clause = ' where %s ' % (sql_condition,)
        ss = self._get_select_clause() + condition_clause
        return self._query(ss,param.mydict())

    def insert(self,rows):
        cursor = self.connect.cursor()
        cursor.executemany(self.sql_insert,[row.mydict() for row in rows])
        self.connect.commit()
        cursor.close()

    def remove(self,rows): #按主键删除
        if self.sql_remove == '':
            return 
        cursor = self.connect.cursor()
        cursor.executemany(self.sql_remove,[row.mydict() for row in rows])
        self.connect.commit()
        cursor.close()

    def remove_by_value(self,vcolumn):
        '''
            根据值删除
            vcolumn包含属性: column,value
        '''
        ss = self._get_delete_clause() + self._get_condition_clause_by_value(vcolumn)
        return self._execute(ss)

    def remove_by_range(self,rcolumn):
        '''
            根据范围删除
            rcolumn包含属性 column,vfrom,vto
        '''
        ss = self._get_delete_clause() + self._get_condition_clause_by_range(rcolumn)
        return self._execute(ss)

    def remove_by_range2(self,rcolumn1,rcolumn2):
        '''
            根据两个范围删除
            rcolumn1,rcolumn2均包含属性column,vfrom,vto
        '''
        ss = self._get_delete_clause() + self._get_condition_clause_by_range2(rcolumn1,rcolumn2)
        return self._execute(ss)

    def remove_by_value_range(self,vcolumn,rcolumn):
        '''
            根据两个范围删除
            rcolumn1,rcolumn2均包含属性column,vfrom,vto
        '''
        ss = self._get_delete_clause() + self._get_condition_clause_by_value_range(vcolumn,rcolumn)
        return self._execute(ss)

    def remove_by_raw(self,sql_condition,param=EMPTY_OBJECT):
        '''
            condition: 如 'itype=:itype'
            param: BaseObject,其__dict__为:{'type':1}
            原始的sql方式
        '''
        
        condition_clause = ' where %s ' % (sql_condition,)
        ss = self._get_delete_clause() + condition_clause
        return self._execute(ss,param.mydict())

    def update(self,rows):#按主键UPDATE
        if self.sql_update == '':
            return 
        cursor = self.connect.cursor()
        #print(self.sql_update)
        cursor.executemany(self.sql_update,[row.mydict() for row in rows])
        self.connect.commit()
        cursor.close()

    def update_by_value(self,vbo,vcolumn):
        '''
            根据值更新
            vcolumn包含属性: column,value
            vbo为包含需要更新的属性的baseobject
        '''
        uclause = self._get_update_clause(vbo)
        if uclause != '':
            ss = uclause + self._get_condition_clause_by_value(vcolumn)
            #print(ss)
            return self._execute(ss)
        else:
            pass

    def update_by_range(self,vbo,rcolumn):
        '''
            根据范围更新
            rcolumn包含属性column,vfrom,vto
            vbo为包含需要更新的属性的baseobject
        '''
        uclause = self._get_update_clause(vbo)
        if uclause != '':
            ss = uclause + self._get_condition_clause_by_range(rcolumn)
            return self._execute(ss)
        else:
            pass

    def update_by_range2(self,vbo,rcolumn1,rcolumn2):
        '''
            根据两个范围更新
            rcolumn1,rcolumn2均包含属性column,vfrom,vto
            vbo为包含需要更新的属性的baseobject
        '''
        uclause = self._get_update_clause(vbo)
        if uclause != '':
            ss = uclause + self._get_condition_clause_by_range2(rcolumn1,rcolumn2)
            return self._execute(ss)
        else:
            pass

    def update_by_value_range(self,vbo,vcolumn,rcolumn):
        '''
            根据两个范围更新
            rcolumn1,rcolumn2均包含属性column,vfrom,vto
            vbo为包含需要更新的属性的baseobject
        '''
        uclause = self._get_update_clause(vbo)
        if uclause != '':
            ss = uclause + self._get_condition_clause_by_value_range(vcolumn,rcolumn)
            return self._execute(ss)
        else:
            pass

    def update_by_raw(self,vbo,sql_condition,param=EMPTY_OBJECT):
        '''
            condition: 如 'itype=:itype'
            param: BaseObject,其__dict__为:{'type':1}
            原始的sql方式
            vbo为包含需要更新的属性的baseobject
        '''
        
        condition_clause = ' where %s ' % (sql_condition,)
        uclause = self._get_update_clause_raw(vbo)
        if uclause != '':
            ss = uclause + condition_clause
            return self._execute(ss,param.mydict())
        else:
            pass

    '''
        内务函数
    '''
    def _query(self,sql_query,param=None):
        #return sql_query
        if self.order_bys:
            sql_query += ' order by ' + self.order_bys
        #print(sql_query)
        cursor = self.connect.cursor()
        if param:
            cursor.execute(sql_query,param)
        else:
            cursor.execute(sql_query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def _execute(self,sql_execute,param=None):
        '''
            无返回的执行
        '''
        cursor = self.connect.cursor()
        if param:
            cursor.execute(sql_execute,param)
        else:
            cursor.execute(sql_execute)
        self.connect.commit()
        cursor.close()

    def _get_select_clause(self):
        return  '''select %s from %s ''' % (self.select_columns,self.sname)

    def _get_delete_clause(self):
        return  '''delete from %s ''' % (self.sname,)

    def _get_update_clause_deprecated(self,vbo):
        for key in vbo.__dict__.keys():  #不更新主键
            assert key not in self.primary_key and key in self.column_map
        supdate = ','.join(['%s = :%s' % (k,k) for k in vbo.__dict__.keys()])
        if supdate:
            return  '''update %s set %s ''' % (self.sname,supdate)
        else:
            return ''

    def _get_update_clause(self,vbo):
        return self._get_update_clause_raw(vbo)

    def _get_update_clause_raw(self,vbo): 
        '''
            因为param被条件语句中的命名参数占用。为避免潜在冲突，update子句中直接用值而不是命名参数
        '''
        for key in vbo.__dict__.keys():  #不更新主键
            assert key not in self.primary_key and key in self.column_map
        supdate = ','.join(['%s = %s' % (item[0],self.column_map[item[0]].tvalue(item[1])) for item in vbo.__dict__.items()])
        if supdate:
            return  '''update %s set %s ''' % (self.sname,supdate)
        else:
            return ''

    def _get_condition_clause_by_value(self,vcolumn):
        assert vcolumn.column.check_type(vcolumn.value)
        ss = '''where %s = %s ''' % (vcolumn.column.name,vcolumn.column.tvalue(vcolumn.value))
        return ss

    def _get_condition_clause_by_range(self,rcolumn):
        assert rcolumn.column.check_type(rcolumn.vfrom) and rcolumn.column.check_type(rcolumn.vto)
        ss = ''' where %s >= %s and %s <= %s ''' % (rcolumn.column.name,rcolumn.column.tvalue(rcolumn.vfrom),rcolumn.column.name,rcolumn.column.tvalue(rcolumn.vto))
        return ss

    def _get_condition_clause_by_range2(self,rcolumn1,rcolumn2):
        '''
            根据两个范围查询
            rcolumn1,rcolumn2均包含属性name,vfrom,vto
        '''
        assert rcolumn1.column.check_type(rcolumn1.vfrom) and rcolumn1.column.check_type(rcolumn1.vto)
        assert rcolumn2.column.check_type(rcolumn2.vfrom) and rcolumn2.column.check_type(rcolumn2.vto)
        ss = ''' where %s >= %s and %s <= %s and %s >=%s and %s <=%s ''' % (
                rcolumn1.column.name,rcolumn1.column.tvalue(rcolumn1.vfrom),rcolumn1.column.name,rcolumn1.column.tvalue(rcolumn1.vto),
                rcolumn2.column.name,rcolumn2.column.tvalue(rcolumn2.vfrom),rcolumn2.column.name,rcolumn2.column.tvalue(rcolumn2.vto),
            )
        return ss

    def _get_condition_clause_by_value_range(self,vcolumn,rcolumn):
        '''
            根据两个范围查询
            vcolumn包含属性: name,value
            rcolumn包含属性name,vfrom,vto
        '''
        assert vcolumn.column.check_type(vcolumn.value)
        assert rcolumn.column.check_type(rcolumn.vfrom) and rcolumn.column.check_type(rcolumn.vto)        
        ss = ''' where %s = %s and %s >=%s and %s <=%s ''' % (
                vcolumn.column.name,vcolumn.column.tvalue(vcolumn.value),
                rcolumn.column.name,rcolumn.column.tvalue(rcolumn.vfrom),rcolumn.column.name,rcolumn.column.tvalue(rcolumn.vto),
            )
        return ss


    #内务函数
    def _check_order_by(self):
        #print(self.columns)
        obs = [(c.order_by,c.name) for c in self.columns if c.order_by>0]
        if obs:
            obs.sort(key=lambda x:x[0])
            return ','.join([b[1] for b in obs])
        elif self.primary_key:
            return ','.join(self.primary_key)
        else:
            return ''

    def _create_sql_create(self):
        t_columns = ['\t,%s %s default %s --%s' % (c.name,c.ctype.stype,c.ctype.default,c.description) for c in self.columns]
        t_columns[0]= '\t ' + t_columns[0][2:]
        s_columns = '\n'.join(t_columns)
        if self.primary_key:
            pkey = ','.join(self.primary_key)
            s_create = '''create table if not exists %s(\n%s\n\t,primary key(%s)\n)''' % (self.sname,s_columns,pkey)
        else:
            s_create = '''create table if not exists %s(\n%s\n)''' % (self.sname,s_columns)
        return s_create

    def _create_sql_index(self,sindex):
        '''
            sindex 为tuple
        '''
        sname = '_'.join([part for part in sindex])
        ss = ','.join([part for part in sindex])
        s_create = 'create index if not exists %s on %s(%s)' % (sname,self.sname,ss)
        return s_create

    def _create_sql_query(self): #返回全部
        ss = 'select %s from %s' % (self.select_columns,self.sname,)
        return ss

    def _create_sql_insert(self):
        dcolumns = ','.join([':'+c.name for c in self.columns])
        ss = 'insert into %s(%s) values(%s)' % (self.sname,self.select_columns,dcolumns)
        return ss

    def _pk_condition(self): #
        conds = [ '%s = :%s' % (pk,pk) for pk in self.primary_key]
        return ' and '.join(conds)

    def _create_sql_update(self):
        normal_columns = [c.name for c in self.columns if c.name not in self.primary_key]
        supdate = ','.join(['%s = :%s' % (nc,nc) for nc in normal_columns])
        spk = self._pk_condition()
        if spk != '' and supdate!='': #存在pk,并且存在pk之外的字段
            ss = 'update %s set %s where %s' % (self.sname,supdate,spk)
        else:#无主键不更新数据
            ss = '' 
        return ss

    def _create_sql_remove(self):
        spk = self._pk_condition()
        if spk != '':
            ss = 'delete from %s where %s' % (self.sname,spk)
        else:   #无主键不删数据
            ss = ''
        return ss


    def _row_factory(self,cursor,row):
        rev = BaseObject()
        for idx, col in enumerate(cursor.description):
            #rev.__dict__[col[0]] = row[idx]
            rev.set_attr(col[0],row[idx])
        return rev

        
'''
example:    #_query 改为直接返回sql_query
import adapter.sqlite.cmapper as sobject
from adapter.sqlite.cmapper import (sobject,CTYPE,column)
so = sobject('test','test',[column('ctest1',CTYPE.Integer,'for test1'),
                                         column('ctest2',CTYPE.Integer,'for test2'),
                                         column('ctest3',CTYPE.String,'for test3'),
                                         column('ctest4',CTYPE.String,'for test4'),
                                         column('ctest5',CTYPE.Integer,'for test5',order_by=10),
                                         column('ctest6',CTYPE.String,'for test6',order_by=20),
                                         column('ctest7',CTYPE.String,'for test7'),
                                    ],
                               primary_key = ('ctest1','ctest2'),
                               indexes = (('ctest2',),('ctest3','ctest4'),('ctest5'))
                )

In [2]: print(so.query_by('itype=:itype,itest=:itest',{'itype':12,'itest':15}))
select ctest1,ctest2,ctest3,ctest4,ctest5,ctest6,ctest7 from test where itype=:i
type,itest=:itest

In [3]: print(so.query())
select ctest1,ctest2,ctest3,ctest4,ctest5,ctest6,ctest7 from test

In [4]: print(so.query_by_range2('ctest5',3,14,'ctest4','123','125'))
select ctest1,ctest2,ctest3,ctest4,ctest5,ctest6,ctest7 from test where :ctest5
>= 3 and :ctest5 <= 14 and :ctest4 >='123' and :ctest4 <='125'

In [5]: print(so.query_by_range('ctest5',3,14))
select ctest1,ctest2,ctest3,ctest4,ctest5,ctest6,ctest7 from test where :ctest5
>= 3 and :ctest5 <= 14

In [6]: print(so.query_by_range('ctest3','3','4'))
select ctest1,ctest2,ctest3,ctest4,ctest5,ctest6,ctest7 from test where :ctest3
>= '3' and :ctest3 <= '4'

In [7]: print(so.query_by_equal('ctest3','3'))
select ctest1,ctest2,ctest3,ctest4,ctest5,ctest6,ctest7 from test where :ctest3
= '3'

In [8]: print(so.query_by_equal('ctest2',3))    #类型错位
select ctest1,ctest2,ctest3,ctest4,ctest5,ctest6,ctest7 from test where :ctest2
= 3

In [9]: print(so.query_by_equal('ctest1',3))
select ctest1,ctest2,ctest3,ctest4,ctest5,ctest6,ctest7 from test where :ctest1
= 3

'''
