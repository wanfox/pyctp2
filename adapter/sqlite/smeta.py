# -*- coding: utf-8 -*-

'''
    交易相关信息类
    不采用直接的 account = sobject(dbname,tname,columns...)方式，是为了后续扩展定制
'''

import math

from adapter.sqlite.cmapper import (sobject,column,CTYPE)

PATH_SQL_META = 'data/sqlite/trade/'

XKEY = 2** 521 -1 #M521 #请务必改成任何一个足够长的数字

class account(sobject):
    def __init__(self):
        columns = [column('name',CTYPE.String,'中文名'),
                   column('ename', CTYPE.String,'名字缩写'),
                   column('acc_type', CTYPE.String,'帐户类型,CMF 国内期货/CMA A股/外汇等, (如果一个帐户支持多个类型,可以用ename分开)'),
                   column('log_name', CTYPE.String,'登录名'),
                   column('passwd', CTYPE.String,'口令'),
                   column('description', CTYPE.String,'描述'),
                ]
        sobject.__init__(self,PATH_SQL_META + 'smeta.db','account',columns,primary_key=('ename',))

    def insert(self,rows):
        for row in rows:
            self._encrypt(row)
        super(account,self).insert(rows)

    def _row_factory(self,cursor,row):
        '''
            口令解密
        '''
        acc = super(account,self)._row_factory(cursor,row)
        self._decrypt(acc)
        return acc

    def _encrypt(self,account):
        salt = hash(account.ename) & 0xFFFF
        v = salt
        for s in account.passwd:
            v =  (v << 16) + ord(s)
        print([ord(s) for s in account.passwd])
        vlen = int(math.log(v)) 
        vp = ((XKEY % (2 ** vlen)) ^ v)
        enkey =  (vp << 16)+ vlen
        print('_encrypt',v,'\nvlen:',vlen,',vp=',vp)
        account.passwd = enkey
        return enkey

    def _decrypt(self,account):
        salt = hash(account.ename) & 0xFFFF
        vlen = account.passwd & 0xFFFF
        vp = account.passwd >> 16
        v = ((XKEY % (2 ** vlen)) ^ vp)
        print('_decrypt',v,'\nvlen:',vlen,',vp=',vp)
        sp = []
        while v > salt:
            sp.insert(0,chr(v&0xFFFF))
            #sp.insert(0,v&0xFFFF)
            v >>= 16
        account.passwd = ''.join(sp)

    

'''
#deprecated #不需要这样一个策略无关的总数
class position(sobject):
    #持仓汇总
    #其中多/空头持仓金额中已经合计昨仓和新仓
    def __init__(self):
        columns = [column('cname',CTYPE.String,'合约名称'),
                   column('nlong',CTYPE.Integer,'多头持仓'),
                   column('nshort',CTYPE.Integer,'空头持仓'),
                   column('cnlong',CTYPE.Integer,'多头新仓'),
                   column('cnshort',CTYPE.Integer,'空头新仓'),
                   column('cndate',CTYPE.Integer,'多头新仓日'),
                   column('cndate',CTYPE.Integer,'空头新仓日'),
                   column('along',CTYPE.Integer,'多头持仓点数'),
                   column('ashort',CTYPE.Integer,'空头持仓点数'),
                ]
        sobject.__init__(self,PATH_SQL_META + 'trader.db','position',columns,primary_key=('cname',))
'''

class position(sobject):
    '''
        持仓明细
    '''
    def __init__(self):
        columns = [column('cname',CTYPE.String,'合约名称'),
                   column('sname',CTYPE.String,'策略名称'),
                   column('direction',CTYPE.Integer,'持仓方向,1=开多,-1=开空'),
                   column('idate',CTYPE.Integer,'开仓日期,YYMMDD'),
                   column('itime',CTYPE.Integer,'开仓时间,HHMMSS'),
                   column('opening_quantity',CTYPE.Integer,'开仓在途数量'),
                   column('opened_quantity',CTYPE.Integer,'已开仓数量'),
                   column('closed_quantity',CTYPE.Integer,'已平数量'),
                   column('closing_quantity',CTYPE.Integer,'平仓在途数量'),
                   column('amount',CTYPE.Integer,'金额'),
                   column('price',CTYPE.Integer,'开仓总点数'),
                   column('price',CTYPE.Integer,'平仓总点数'),
                   column('stop_price',CTYPE.Integer,'止损价格'),
                ]
        sobject.__init__(self,PATH_SQL_META + 'trader.db','position_detail',columns,primary_key=('cname','idate','itime','direction',))


class strategy_position(sobject):
    '''
        策略头寸
    '''
    def __init__(self):
        columns = [column('cname',CTYPE.String,'合约名称'),
                   column('sname',CTYPE.String,'策略名称'),
                   column('stag',CTYPE.String,'策略tag,即合约名的#连接'),
                   column('nlong',CTYPE.Integer,'多头持仓'),
                   column('nshort',CTYPE.Integer,'空头持仓'),
                   column('nlong_onthefly',CTYPE.Integer,'多头在途,负数表示在平仓在途'),
                   column('nshort_onthefly',CTYPE.Integer,'空头在途,负数表示在平仓在途'),
                   column('nlong_remainder',CTYPE.Integer,'多头余仓，1/10000单位，因为套利比例导致的未开余数，必然<10000,预计在后续对齐'),
                   column('nshort_remainder',CTYPE.Integer,'空头持仓，1/10000单位，因为套利比例导致的未开余数，必然<10000,预计在后续对齐'),
                ]
        sobject.__init__(self,PATH_SQL_META + 'trader.db','strategy_position',columns,primary_key=('cname','sname','stag',))


class strategy(sobject):
    '''
        策略概况
    '''

    def __init__(self):
        columns = [column('sname',CTYPE.String,'策略名称'),
                   column('stag',CTYPE.String,'策略tag,即合约名的#连接'),
                   column('dlong',CTYPE.Integer,'多头总金额, 日结时计算'),
                   column('dshort',CTYPE.Integer,'空头总金额,日结时计算'),
                   column('volatility',CTYPE.Integer,'波动率，日结时计算'),
                   column('parameters',CTYPE.String,'参数列表，以dict方式表示'),
                ]
        sobject.__init__(self,PATH_SQL_META + 'trader.db','position_detail',columns,primary_key=('sname','stag',))

