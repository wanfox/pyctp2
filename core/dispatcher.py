# -*- coding: utf-8 -*-

from dac2 import XMINUTE

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
        chooks[ctype].append(callback)
        if cname not in self.data:
            self.data[cname] = BaseObject(ticks=[],min1s=[],dates=[],cached=BaseObject())   #cached用于缓存合约相关的持久数据
        
    def unregister(self,cname,ctype,callback):
        chooks = self.c2hooks.setdefault(cname,hooks())
        try:
            chooks[ctype].remove(callback)
        except Exception as inst: #要移除的callback不存在
            pass

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
        cdata.ticks.append(ctick)
        cdata.cached.ctick = ctick
        xm = XMINUTE(cdata.cached)  #这里对XMINUTE的调用及其内部有足够的依赖与纠结
        for gback in self.ghooks[CTYPE_TICK]:
            gback(self.data[ctick.cname],ctick)
        chooks = self.c2hooks[ctick.cname]
        for cback in chooks[CTYPE_TICK]:
            cback(self.data[ctick.cname],ctick)
        if xm.modified:
            self.xmin(xm.cmin1)

    def xmin(self,cmin1):
        '''
            新分钟数据到来
        '''
        cdata = self.data[ctick.cname]
        cdata.min1s.append(cmin1)
        for gback in self.ghooks[CTYPE_MIN]:
            gback(self.data[cmin1.cname],cmin1)
        chooks = self.c2hooks[ctick.cname]
        for cback in chooks[CTYPE_MIN]:
            cback(self.data[cmin1.cname],cmin1)


