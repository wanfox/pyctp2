# -*- coding: utf-8 -*-

'''
按分钟同步保存几个合约的数据
'''

import copy

from core import base

import core.dispatcher as dispatcher

import adapter.sqlite as s3

class recorder(object):
    def __init__(self,cnames):
        self.min1s = []
        self.cnames = cnames #
        self.min1ss = []
        self.cur_min = base.BaseObject(idate=0,imin = 0)
        for cname in self.cnames:   #初始化
            self.cur_min.__dict__[cname] = base.create_sep_minute(cname)


    def prepare_data(self,syear='2013'):
        conn = s3.connect_min1_db(syear)
        rs = s3.query_min(conn,self.cnames)
        conn.close()
        return rs

    def on_min1(self,data,cmin1):
        #print(cmin1.imin,self.cur_min.imin)
        if cmin1.imin != self.cur_min.imin and self.cur_min.imin > 0:
            self.min1ss.append(self.cur_min)
            self.cur_min = copy.copy(self.cur_min)
            #这样,如果本分钟某合约无数据,可延续上一数据. 并且知道延续的是哪一期
            self.cur_min.idate = cmin1.idate
            self.cur_min.imin = cmin1.imin
        self.cur_min.__dict__[cmin1.cname] = cmin1
        self.cur_min.imin = cmin1.imin

    def hook(self,cdispatcher):
        for cname in self.cnames:
            cdispatcher.register(cname,dispatcher.CTYPE_MIN,self.on_min1)


def smain():
    cdispatcher = dispatcher.dispatcher()
    crecorder = recorder(['y1309','p1309','OI1309'])
    crecorder.hook(cdispatcher)
    conn = s3.connect_min1_db(2013)
    rs = s3.query_min(conn,crecorder.cnames)
    conn.close()
    for row in rs:
        cdispatcher.xmin(row)
    cdispatcher.min_fence()

    #print(len(crecorder.min1ss))
    #for b3 in crecorder.min1ss[-30:]:
    #    print(b3.y1309,b3.p1309,b3.OI1309)    
    return crecorder

SYNC_SAVER_PATH = 'data/examples/sync_saver_'
def saver(fname,crecorder):
    path_name = SYNC_SAVER_PATH + fname + '.csv'
    with open(path_name,'w') as ff:
        i = 1
        ##表头
        ff.write('序号,日期,时间')
        for cname in crecorder.cnames:
            ff.write(',%s' % (cname,))
        ff.write('\n')
        ##内容
        for cmins in crecorder.min1ss:
            if cmins.idate < 20130101:
                continue
            ff.write('%d,%d,%d' % (i,cmins.idate,cmins.imin))
            for cname in crecorder.cnames:
                ff.write(',%d' % (cmins.__dict__[cname].iclose,))
                #print(cname,cmins.__dict__)
            ff.write('\n')
            i += 1


if __name__ == '__main__':
    ''' In [1]: import examples.sync_saver as saver
        In [2]: rec = saver.smain()
        In [3]: saver.saver('ypOI1309',rec)
    '''
    import examples.sync_saver as saver
    rec = saver.smain()
    saver.saver('ypOI1309',rec)


