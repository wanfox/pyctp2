# -*-coding:utf-8 -*-

import unittest

import base
from dac2 import *

class ModuleTest(unittest.TestCase):

    ###基本运算
    def test_oper1(self):   #测试NEG
        self.assertEquals([],NEG([]))
        a = [1,2,-3,4,-5,6]
        self.assertEquals([-1,-2,3,-4,5,-6],NEG(a))
        a.append(7)
        a.append(-8)
        self.assertEquals([-1,-2,3,-4,5,-6,-7,8],NEG(a))


    def test_oper2(self):   #测试ADD
        self.assertEquals([],ADD([],[]))
        a = [1,2,3,4,5,6]
        b = [10,20,30,40,50,60]
        self.assertEquals([11,22,33,44,55,66],ADD(a,b))
        a.append(7)
        b.append(70)
        self.assertEquals([11,22,33,44,55,66,77],ADD(a,b))

    def test_oper21(self):   #测试ADD
        self.assertEquals([],ADD1([],0))
        a = [1,2,3,4,5,6]
        self.assertEquals([3,4,5,6,7,8],ADD1(a,2))
        a.append(7)
        self.assertEquals([4,5,6,7,8,9,10],ADD1(a,3))

    def test_and(self):   #测试AND
        self.assertEquals([],AND([],[]))
        a = [1,2,3,4,0,6]
        b = [10,-20,30,40,50,60]
        self.assertEquals([True,True,True,True,False,True],AND(a,b))
        a.append(7)
        b.append(70)
        self.assertEquals([True,True,True,True,False,True,True],AND(a,b))
        a.append(9)
        b.append(0)
        self.assertEquals([True,True,True,True,False,True,True,False],AND(a,b))

    def test_gand(self):   #测试AND
        self.assertEquals([],GAND([],[]))
        a = [1,2,3,4,0,6]
        b = [10,-20,30,40,50,60]
        self.assertEquals([True,True,True,True,False,True],GAND(a,b))
        a.append(7)
        b.append(70)
        self.assertEquals([True,True,True,True,False,True,True],GAND(a,b))
        a.append(9)
        b.append(0)
        self.assertEquals([True,True,True,True,False,True,True,False],GAND(a,b))

    def test_gor(self):   #测试AND
        self.assertEquals([],GOR([],[]))
        a = [1,0,3,0,0,6]
        b = [10,-2,30,0,50,60]
        self.assertEquals([True,True,True,False,True,True],GOR(a,b))
        a.append(0)
        b.append(70)
        self.assertEquals([True,True,True,False,True,True,True],GOR(a,b))
        a.append(0)
        b.append(0)
        self.assertEquals([True,True,True,False,True,True,True,False],GOR(a,b))


    def test_DIV(self):   
        self.assertEquals([],DIV([],[]))
        a = [10,20,30,15,50,30]
        b = [1,2,3,4,0,6]
        self.assertEquals([10,10,10,4,50000,5],DIV(a,b))
        a.append(7)
        b.append(70)
        self.assertEquals([10,10,10,4,50000,5,0],DIV(a,b))

    def test_DIV1(self):   
        self.assertEquals([],DIV1([],12))
        a = [10,4,30,15,50,30]
        self.assertEquals([1,0,3,2,5,3],DIV1(a,10))
        a.append(7)
        self.assertEquals([1,0,3,2,5,3,1],DIV1(a,10))


    ##常用指标
    def test_sum(self):
        self.assertEquals([],ACCUMULATE([]))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEquals([1,3,6,10,15,21,28,36,45,45],ACCUMULATE(a))
        a.append(100)
        self.assertEquals([1,3,6,10,15,21,28,36,45,45,145],ACCUMULATE(a))

    def test_msum(self):
        self.assertEquals([],MSUM([],2))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEquals([1,2,3,4,5,6,7,8,9,0],MSUM(a,1))
        self.assertEquals([1,3,5,7,9,11,13,15,17,9],MSUM(a,2))
        a.append(100)
        self.assertEquals([1,3,5,7,9,11,13,15,17,9,100],MSUM(a,2))


    def test_ma(self):
        self.assertEquals([],MA([],3))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEquals([1,2,2,3,4,5,6,7,8,6],MA(a,3))
        a.append(100)
        self.assertEquals([1,2,2,3,4,5,6,7,8,6,36],MA(a,3))

    def test_nma(self):
        self.assertEquals([],NMA([]))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEquals([1,2,2,3,3,4,4,5,5,5],NMA(a))
        a.append(100)
        self.assertEquals([1,2,2,3,3,4,4,5,5,5,13],NMA(a))

    def test_nsum(self):
        self.assertEquals([],NSUM([]))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEquals([1,3,6,10,15,21,28,36,45,45],NSUM(a))
        a.append(100)
        self.assertEquals([1,3,6,10,15,21,28,36,45,45,145],NSUM(a))

    def test_cexpma(self):
        self.assertEquals([],CEXPMA([],6))
        source = [25000,24875,24781,24594,24500,24625,25219,27250]
        self.assertEquals([25000,24958,24899,24797,24698,24674,24856,25654],CEXPMA(source,5))   #相当于5日
        source.append(200000)
        self.assertEquals([25000,24958,24899,24797,24698,24674,24856,25654,83769],CEXPMA(source,5))   #相当于5日


    def test_tr(self):
        self.assertEquals([],TR([],[],[]))
        shigh = [200,250,200,400]
        slow = [100,200,100,200]
        sclose = [150,220,150,300]
        self.assertEquals([100*XBASE,100*XBASE,120*XBASE,250*XBASE],TR(sclose,shigh,slow))
        shigh.append(1000)
        slow.append(500)
        sclose.append(700)
        self.assertEquals([100*XBASE,100*XBASE,120*XBASE,250*XBASE,700*XBASE],TR(sclose,shigh,slow))

    def test_atr(self):
        shigh = [200,250,200,400]
        slow = [100,200,100,200]
        sclose = [150,220,150,300]
        self.assertEquals([100*XBASE,100*XBASE,120*XBASE,250*XBASE],ATR(sclose,shigh,slow,1))
        shigh.append(1000)
        slow.append(500)
        sclose.append(700)
        self.assertEquals([100*XBASE,100*XBASE,120*XBASE,250*XBASE,700*XBASE],ATR(sclose,shigh,slow,1))

    def test_xatr(self):
        self.assertEquals([],XATR([],[],[]))
        shigh = [200,250,200,400]
        slow = [100,200,100,200]
        sclose = [150,220,150,300]
        self.assertEquals([666667,454545,679333,386667],XATR(sclose,shigh,slow))
        shigh.append(1000)
        slow.append(500)
        sclose.append(700)
        self.assertEquals([666667,454545,679333,386667,245171],XATR(sclose,shigh,slow))

    def test_strend(self):
        self.assertEquals([],STREND([]))
        self.assertEquals([0],STREND([1]))        
        source = [10,20,30,30,40,50,40,30,20,20,10,20]
        self.assertEquals([0,1,2,3,4,5,-1,-2,-3,-4,-5,1],STREND(source))
        source.append(20)
        self.assertEquals([0,1,2,3,4,5,-1,-2,-3,-4,-5,1,2],STREND(source))
        source.append(30)
        self.assertEquals([0,1,2,3,4,5,-1,-2,-3,-4,-5,1,2,3],STREND(source))
        source.append(20)
        self.assertEquals([0,1,2,3,4,5,-1,-2,-3,-4,-5,1,2,3,-1],STREND(source))
        source.append(10)
        self.assertEquals([0,1,2,3,4,5,-1,-2,-3,-4,-5,1,2,3,-1,-2],STREND(source))


    def test_tmax(self):
        self.assertEquals([],TMAX([],10))
        source = [10,12,3,2,5,100,0,13,16,9]
        self.assertEquals([10,12,3,2,5,100,0,13,16,9],TMAX(source,1))
        self.assertEquals([10,12,12,3,5,100,100,13,16,16],TMAX(source,2))
        source.append(3)
        source.append(30)
        self.assertEquals([10,12,12,3,5,100,100,13,16,16,9,30],TMAX(source,2))
        self.assertEquals([10,12,12,12,5,100,100,100,16,16,16,30],TMAX(source,3))

    def test_tmin(self):
        self.assertEquals([],TMIN([],10))
        source = [10,12,3,2,5,100,0,13,16,9]
        self.assertEquals([10,12,3,2,5,100,0,13,16,9],TMIN(source,1))
        self.assertEquals([10,10,3,2,2,5,0,0,13,9],TMIN(source,2))
        source.append(3)
        source.append(30)
        self.assertEquals([10,10,3,2,2,5,0,0,13,9,3,3],TMIN(source,2))
        self.assertEquals([10,10,3,2,2,2,0,0,0,9,3,3],TMIN(source,3))

    def test_nmax(self):
        self.assertEquals([],NMAX([]))
        source = [10,12,3,2,5,100,0,13,16,9]
        self.assertEquals([10,12,12,12,12,100,100,100,100,100],NMAX(source))
        source.append(3)
        source.append(103)
        self.assertEquals([10,12,12,12,12,100,100,100,100,100,100,103],NMAX(source))

    def test_nmin(self):
        self.assertEquals([],NMIN([]))
        source = [10,12,3,2,5,100,0,13,16,9]
        self.assertEquals([10,10,3,2,2,2,0,0,0,0],NMIN(source))
        source.append(3)
        source.append(-1)
        self.assertEquals([10,10,3,2,2,2,0,0,0,0,0,-1],NMIN(source))


    def test_cross(self):   #
        self.assertEquals([],UPCROSS([],[]))
        target = [10,20,30,40,50,40,30,20,10,12,11,12]
        follow = [5,15,35,41,60,50,25,26,8,12,13,12]
        self.assertEquals([0,0,1,0,0,0,0,1,0,0,1,0],UPCROSS(target,follow))
        self.assertEquals([1,0,0,0,0,0,1,0,1,0,0,0],DOWNCROSS(target,follow))
        target.append(15)
        follow.append(11)
        self.assertEquals([0,0,1,0,0,0,0,1,0,0,1,0,0],UPCROSS(target,follow))
        self.assertEquals([1,0,0,0,0,0,1,0,1,0,0,0,1],DOWNCROSS(target,follow))
        target.append(13)
        follow.append(25)
        self.assertEquals([0,0,1,0,0,0,0,1,0,0,1,0,0,1],UPCROSS(target,follow))
        self.assertEquals([1,0,0,0,0,0,1,0,1,0,0,0,1,0],DOWNCROSS(target,follow))

    def test_ncross(self):   #
        self.assertEquals([],NUPCROSS([],10))
        follow = [5,15,35,41,60,50,25,26,8,12,13,12]
        self.assertEquals([0,1,0,0,0,0,0,0,0,1,0,0],NUPCROSS(follow,10))
        self.assertEquals([1,0,0,0,0,0,0,0,1,0,0,0],NDOWNCROSS(follow,10))
        follow.append(8)
        self.assertEquals([0,1,0,0,0,0,0,0,0,1,0,0,0],NUPCROSS(follow,10))
        self.assertEquals([1,0,0,0,0,0,0,0,1,0,0,0,1],NDOWNCROSS(follow,10))
        follow.append(25)
        self.assertEquals([0,0,1,0,0,0,0,0,0,0,0,0,0,1],NUPCROSS(follow,15))
        self.assertEquals([1,0,0,0,0,0,0,0,1,0,0,0,1,0],NDOWNCROSS(follow,10))


    def test_ref(self):
        self.assertEquals([],REF([]))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEquals([1,2,3,4,5,6,7,8,9,0],REF(a,0))
        self.assertEquals([1,1,2,3,4,5,6,7,8,9],REF(a,1))
        self.assertEquals([1,1,1,2,3,4,5,6,7,8],REF(a,2))
        self.assertEquals([1,1,1,1,1,1,1,1,1,2],REF(a,8))
        self.assertEquals([1,1,1,1,1,1,1,1,1,1],REF(a,9))
        self.assertEquals([1,1,1,1,1,1,1,1,1,1],REF(a,10))
        self.assertEquals([1,1,1,1,1,1,1,1,1,1],REF(a,11))
        self.assertEquals([1,1,1,1,1,1,1,1,1,1],REF(a,100))
        a.append(100)
        self.assertEquals([1,1,1,2,3,4,5,6,7,8,9],REF(a,2))

    def test_minute_1(self):
        m1 = MINUTE_1([])
        self.assertEquals([],m1.sclose)

        ticks = [TICK(),TICK(),TICK(),TICK()]
        ticks[0].price = 100
        ticks[0].time = 91400000
        ticks[0].date = 20120111
        ticks[0].dvolume = 10
        ticks[0].holding = 10
        ticks[0].min1 = time2min(ticks[0].time)
        ticks[1].price = 110
        ticks[1].time = 91500000
        ticks[1].date = 20120111
        ticks[1].min1 = time2min(ticks[1].time)
        ticks[1].dvolume = 30
        ticks[1].holding = 11
        ticks[2].price = 115
        ticks[2].time = 91501000
        ticks[2].date = 20120111
        ticks[2].dvolume = 50
        ticks[2].holding = 12
        ticks[2].min1 = time2min(ticks[2].time)
        ticks[3].price = 91
        ticks[3].time = 91600000
        ticks[3].date = 20120111
        ticks[3].dvolume = 51
        ticks[3].holding = 13
        ticks[3].min1 = time2min(ticks[3].time)
        m2 = MINUTE_1(ticks)
        self.assertEquals(2,len(m2.sclose))
        self.assertEquals([100,115],m2.sclose)
        self.assertEquals([100,110],m2.slow)
        self.assertEquals([100,115],m2.shigh)
        self.assertEquals([914,915],m2.stime)
        self.assertEquals([0,40],m2.svol)
        self.assertTrue(m2.modified)
        #
        ticks.extend([TICK(),TICK(),TICK()])
        ticks[4].price = 93
        ticks[4].time = 91601000
        ticks[4].date = 20120111
        ticks[4].min1 = time2min(ticks[4].time)
        ticks[4].dvolume = 80
        ticks[4].holding = 10
        ticks[5].price = 90
        ticks[5].time = 91602000
        ticks[5].date = 20120111
        ticks[5].dvolume = 88
        ticks[5].holding = 10
        ticks[5].min1 = time2min(ticks[5].time)
        ticks[6].price = 90
        ticks[6].time = 91700000
        ticks[6].date = 20120111
        ticks[6].dvolume = 89
        ticks[6].holding = 10
        ticks[6].min1 = time2min(ticks[6].time)
        m2 = MINUTE_1(ticks)
        self.assertEquals([100,115,90],m2.sclose)
        self.assertEquals([100,110,90],m2.slow)
        self.assertEquals([100,115,93],m2.shigh)
        self.assertEquals([914,915,916],m2.stime)
        self.assertEquals([0,40,38],m2.svol)
        self.assertTrue(m2.modified)
        ##
        ticks.append(TICK())
        ticks[7].price = 91
        ticks[7].time = 91701000
        ticks[7].date = 20120111
        ticks[7].dvolume = 81
        ticks[7].holding = 10
        ticks[7].min1 = time2min(ticks[7].time)
        m2 = MINUTE_1(ticks)
        self.assertFalse(m2.modified)
        self.assertEquals([914,915,916],m2.stime)
        ##测试终结符
        ticks.append(TICK())
        ticks[-1].price = 0
        ticks[-1].time = 0
        ticks[-1].date = 0
        ticks[-1].dvolume = 0
        ticks[-1].holding = 0
        ticks[-1].min1 = time2min(ticks[-1].time)
        m2 = MINUTE_1(ticks)
        self.assertTrue(m2.modified)
        self.assertEquals([914,915,916,917],m2.stime)
        ##测试重复的终结符
        ticks.append(TICK())
        ticks[-1].price = 0
        ticks[-1].time = 0
        ticks[-1].date = 0
        ticks[-1].dvolume = 0
        ticks[-1].holding = 0
        ticks[-1].min1 = time2min(ticks[-1].time)
        m2 = MINUTE_1(ticks)
        self.assertFalse(m2.modified)   #无变化
        self.assertEquals([914,915,916,917],m2.stime)
        ##测试pre_min
        pre_min1 = BaseObject(sopen=[1],sclose=[10],shigh=[13],slow=[0],svol=[1000],stime=[919],sholding=[101],sdate=[914])
        ticks = ticks[:4]
        mm = MINUTE_1(ticks,pre_min1=pre_min1)
        self.assertEquals([10,100,115],mm.sclose)
        self.assertEquals([919,914,915],mm.stime)
    def test_minute(self):
        m1 = MINUTE([],[],[],[],[])
        self.assertEquals([],m1.sclose)
        prices = [100,110,115,91]
        times = [91400000,91500000,91501000,91600000]
        dates = [20120111,20120111,20120111,20120111]
        dvols = [10,30,50,51]
        holdings = [10,11,12,13]
        m2 = MINUTE(dates,times,prices,dvols,holdings)
        self.assertEquals(2,len(m2.sclose))
        self.assertEquals([100,115],m2.sclose)
        self.assertEquals([100,110],m2.slow)
        self.assertEquals([100,115],m2.shigh)
        self.assertEquals([914,915],m2.stime)
        self.assertEquals([0,40],m2.svol)
        self.assertTrue(m2.modified)
        prices.extend([93,90,90])
        times.extend([91601000,91602000,91700000])
        dates.extend([20120111,20120111,20120111])
        dvols.extend([80,88,89])
        holdings.extend([10,10,10])
        m2 = MINUTE(dates,times,prices,dvols,holdings)
        self.assertEquals([100,115,90],m2.sclose)
        self.assertEquals([100,110,90],m2.slow)
        self.assertEquals([100,115,93],m2.shigh)
        self.assertEquals([914,915,916],m2.stime)
        self.assertEquals([0,40,38],m2.svol)
        self.assertTrue(m2.modified)
        prices.append(91)
        times.append(91701000)
        dates.append(20120111)
        dvols.append(81)
        holdings.append(10)
        m2 = MINUTE(dates,times,prices,dvols,holdings)
        self.assertFalse(m2.modified)
        ##测试终结符
        prices.extend([0])
        times.extend([0])
        dates.extend([0])
        dvols.extend([0])
        holdings.extend([0])
        m2 = MINUTE(dates,times,prices,dvols,holdings)
        self.assertTrue(m2.modified)
        self.assertEquals([914,915,916,917],m2.stime)
        ##测试重复的终结符
        prices.extend([0])
        times.extend([0])
        dates.extend([0])
        dvols.extend([0])
        holdings.extend([0])
        m2 = MINUTE(dates,times,prices,dvols,holdings)
        self.assertFalse(m2.modified)   #无变化
        self.assertEquals([914,915,916,917],m2.stime)
        ###
        pre_min1 = BaseObject(sopen=[1],sclose=[10],shigh=[13],slow=[0],svol=[1000],stime=[919],sholding=[101],sdate=[914])
        prices = [100,110,115,91]
        times = [91500000,91600000,91601000,91700000]
        dates = [20120111,20120111,20120111,20120111]
        dvols = [10,30,50,51]
        holdings = [10,11,12,13]
        mm = MINUTE(dates,times,prices,dvols,holdings,pre_min1=pre_min1)
        self.assertEquals([10,100,115],mm.sclose)




if __name__ == "__main__":
    import logging
    logging.basicConfig(filename="test.log",level=logging.DEBUG,format='%(name)s:%(funcName)s:%(lineno)d:%(asctime)s %(levelname)s %(message)s')
    
    unittest.main()
