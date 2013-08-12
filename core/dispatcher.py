# -*- coding: utf-8 -*-

from core.dac2 import XMINUTE
from core.base import ( BaseObject,
                        create_sep_tick,
                        create_sep_minute,
                    )

CTYPE_TICK = 1
CTYPE_MIN = 2

class hooks(object):
    def __init__(self):
        self.hooks = {CTYPE_TICK:[],CTYPE_MIN:[]}

class dispatcher(object):
    def __init__(self):
        self.c2hooks = {} #cname ==> hook
        self.ghooks = hooks()
        self.env = BaseObject(data={})
        self.data = self.env.data

    def register(self,cname,ctype,callback):
        '''
            ctype 必须为 CTYPE_XXX
        '''
        chooks = self.c2hooks.setdefault(cname,hooks())
        chooks.hooks[ctype].append(callback)
        if cname not in self.data:
            self.data[cname] = BaseObject(ticks=[],min1s=[],dates=[],cached=BaseObject())   #cached用于缓存合约相关的持久数据
            self.data[cname].smin1 = BaseObject(sopen=[],sclose=[],shigh=[],slow=[],svolume=[],sholding=[],stype=[])    #分钟序列数据
            self.data[cname].stick = BaseObject(sprice=[],sdvolume=[])   #tick序列数据
        
    def unregister(self,cname,ctype,callback):
        chooks = self.c2hooks.setdefault(cname,hooks())
        try:
            chooks.hooks[ctype].remove(callback)
        except Exception as inst: #要移除的callback不存在
            pass

    def day_reset(self):
        for cname in self.data:
            self.data[cname].stick.sprice = []  #清空ticks数据
            self.data[cname].stick.sdvolume = []

    def init_ticks(cname,ticks):    #设定已发生的ticks数据
        assert cname in self.data
        self.data.ticks = ticks

    def init_min1s(cname,min1s):    #设定已发生的分钟数据
        assert cname in self.data
        self.data.min1s = min1s

    def init_dates(cname,dates):    #设定已发生的日数据
        assert cname in self.data
        self.data.dates = dates

    def register_global(self,ctype,callback):
        '''
            全局hook被所有合约调用. 且无撤销方式
            1.用于从tick计算分钟数据,以及从分钟计算X分钟数据
            2.日轮换时清除ticks数据
        '''
        self.ghooks[ctype].append(callback)

    def xtick(self,ctick):
        '''
            新TICK到来. 这里并不过滤重复tick, 由调用者保证不重复
        '''
        if ctick.cname not in self.c2hooks:  #不在盯盘列表中
            return
        ##必须环节
        cdata = self.data[ctick.cname]
        if ctick.min1 > 0:  
            cdata.ticks.append(ctick)
            cdata.stick.sprice.append(ctick.price)
            cdata.stick.sdvolume.append(ctick.dvolume)
        cdata.cached.ctick = ctick
        #隔断分钟需要传入后续处理中, 目前用处只有便于XMINUTE识别日结束
        xm = XMINUTE(cdata.cached)  #这里对XMINUTE的调用及其内部有足够的依赖与纠结. 传入cached是为了保持在各次调用中这个参数id是一致的,以对付缓存机制
        for gback in self.ghooks.hooks[CTYPE_TICK]:
            gback(self.data[ctick.cname],ctick)
        chooks = self.c2hooks[ctick.cname].hooks
        for cback in chooks[CTYPE_TICK]:
            cback(self.data[ctick.cname],ctick)
        if xm.modified:
            self.xmin(xm.cmin1)

    def xmin(self,cmin1):
        '''
            新分钟数据到来
        '''
        cdata = self.data[cmin1.cname]
        if cmin1.imin > 0:
            cdata.min1s.append(cmin1)
            cdata.smin1.sopen.append(cmin1.iopen)
            cdata.smin1.sclose.append(cmin1.iclose)
            cdata.smin1.shigh.append(cmin1.ihigh)
            cdata.smin1.slow.append(cmin1.ilow)
            cdata.smin1.svolume.append(cmin1.ivolume)
            cdata.smin1.sholding.append(cmin1.iholding)
            cdata.smin1.stype.append(cmin1.itype)
        else: #日隔断
            self.day_reset()
        #隔断分钟需要传入hook中, 目前用处只有便于hook识别日结束,以保存数据
        for gback in self.ghooks.hooks[CTYPE_MIN]:
            gback(self.data[cmin1.cname],cmin1)
        chooks = self.c2hooks[cmin1.cname].hooks
        for cback in chooks[CTYPE_MIN]:
            cback(self.data[cmin1.cname],cmin1)

    def tick_fence(self):    #用于隔离TICK,完成日收束.  仅用在回测中,实盘用tick_fence
        for cname in self.data:
            sep_tick = create_sep_tick(cname)
            self.xtick(sep_tick)

    def min_fence(self):    #用于隔离分钟,完成日收束.  仅用在回测中,实盘用tick_fence
        for cname in self.data:
            sep_min = create_sep_minute(cname)
            self.xmin(sep_min)
        
