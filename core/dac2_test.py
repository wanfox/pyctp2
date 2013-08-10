# -*-coding:utf-8 -*-

import unittest

import base
from dac2 import *

class ModuleTest(unittest.TestCase):

    ###基本运算
    def test_oper1(self):   #测试NEG
        self.assertEqual([],NEG([]))
        a = [1,2,-3,4,-5,6]
        self.assertEqual([-1,-2,3,-4,5,-6],NEG(a))
        a.append(7)
        a.append(-8)
        self.assertEqual([-1,-2,3,-4,5,-6,-7,8],NEG(a))


    def test_oper2(self):   #测试ADD
        self.assertEqual([],ADD([],[]))
        a = [1,2,3,4,5,6]
        b = [10,20,30,40,50,60]
        self.assertEqual([11,22,33,44,55,66],ADD(a,b))
        a.append(7)
        b.append(70)
        self.assertEqual([11,22,33,44,55,66,77],ADD(a,b))

    def test_oper21(self):   #测试ADD
        self.assertEqual([],ADD1([],0))
        a = [1,2,3,4,5,6]
        self.assertEqual([3,4,5,6,7,8],ADD1(a,2))
        a.append(7)
        self.assertEqual([4,5,6,7,8,9,10],ADD1(a,3))

    def test_and(self):   #测试AND
        self.assertEqual([],AND([],[]))
        a = [1,2,3,4,0,6]
        b = [10,-20,30,40,50,60]
        self.assertEqual([True,True,True,True,False,True],AND(a,b))
        a.append(7)
        b.append(70)
        self.assertEqual([True,True,True,True,False,True,True],AND(a,b))
        a.append(9)
        b.append(0)
        self.assertEqual([True,True,True,True,False,True,True,False],AND(a,b))

    def test_gand(self):   #测试AND
        self.assertEqual([],GAND([],[]))
        a = [1,2,3,4,0,6]
        b = [10,-20,30,40,50,60]
        self.assertEqual([True,True,True,True,False,True],GAND(a,b))
        a.append(7)
        b.append(70)
        self.assertEqual([True,True,True,True,False,True,True],GAND(a,b))
        a.append(9)
        b.append(0)
        self.assertEqual([True,True,True,True,False,True,True,False],GAND(a,b))

    def test_gor(self):   #测试AND
        self.assertEqual([],GOR([],[]))
        a = [1,0,3,0,0,6]
        b = [10,-2,30,0,50,60]
        self.assertEqual([True,True,True,False,True,True],GOR(a,b))
        a.append(0)
        b.append(70)
        self.assertEqual([True,True,True,False,True,True,True],GOR(a,b))
        a.append(0)
        b.append(0)
        self.assertEqual([True,True,True,False,True,True,True,False],GOR(a,b))


    def test_DIV(self):   
        self.assertEqual([],DIV([],[]))
        a = [10,20,30,15,50,30]
        b = [1,2,3,4,0,6]
        self.assertEqual([10,10,10,4,50000,5],DIV(a,b))
        a.append(7)
        b.append(70)
        self.assertEqual([10,10,10,4,50000,5,0],DIV(a,b))

    def test_DIV1(self):   
        self.assertEqual([],DIV1([],12))
        a = [10,4,30,15,50,30]
        self.assertEqual([1,0,3,2,5,3],DIV1(a,10))
        a.append(7)
        self.assertEqual([1,0,3,2,5,3,1],DIV1(a,10))


    ##常用指标
    def test_sum(self):
        self.assertEqual([],ACCUMULATE([]))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEqual([1,3,6,10,15,21,28,36,45,45],ACCUMULATE(a))
        a.append(100)
        self.assertEqual([1,3,6,10,15,21,28,36,45,45,145],ACCUMULATE(a))

    def test_msum(self):
        self.assertEqual([],MSUM([],2))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEqual([1,2,3,4,5,6,7,8,9,0],MSUM(a,1))
        self.assertEqual([1,3,5,7,9,11,13,15,17,9],MSUM(a,2))
        a.append(100)
        self.assertEqual([1,3,5,7,9,11,13,15,17,9,100],MSUM(a,2))


    def test_ma(self):
        self.assertEqual([],MA([],3))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEqual([1,2,2,3,4,5,6,7,8,6],MA(a,3))
        a.append(100)
        self.assertEqual([1,2,2,3,4,5,6,7,8,6,36],MA(a,3))

    def test_nma(self):
        self.assertEqual([],NMA([]))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEqual([1,2,2,3,3,4,4,5,5,5],NMA(a))
        a.append(100)
        self.assertEqual([1,2,2,3,3,4,4,5,5,5,13],NMA(a))

    def test_nsum(self):
        self.assertEqual([],NSUM([]))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEqual([1,3,6,10,15,21,28,36,45,45],NSUM(a))
        a.append(100)
        self.assertEqual([1,3,6,10,15,21,28,36,45,45,145],NSUM(a))

    def test_cexpma(self):
        self.assertEqual([],CEXPMA([],6))
        source = [25000,24875,24781,24594,24500,24625,25219,27250]
        self.assertEqual([25000,24958,24899,24797,24698,24674,24856,25654],CEXPMA(source,5))   #相当于5日
        source.append(200000)
        self.assertEqual([25000,24958,24899,24797,24698,24674,24856,25654,83769],CEXPMA(source,5))   #相当于5日


    def test_tr(self):
        self.assertEqual([],TR([],[],[]))
        shigh = [200,250,200,400]
        slow = [100,200,100,200]
        sclose = [150,220,150,300]
        self.assertEqual([100*XBASE,100*XBASE,120*XBASE,250*XBASE],TR(sclose,shigh,slow))
        shigh.append(1000)
        slow.append(500)
        sclose.append(700)
        self.assertEqual([100*XBASE,100*XBASE,120*XBASE,250*XBASE,700*XBASE],TR(sclose,shigh,slow))

    def test_atr(self):
        shigh = [200,250,200,400]
        slow = [100,200,100,200]
        sclose = [150,220,150,300]
        self.assertEqual([100*XBASE,100*XBASE,120*XBASE,250*XBASE],ATR(sclose,shigh,slow,1))
        shigh.append(1000)
        slow.append(500)
        sclose.append(700)
        self.assertEqual([100*XBASE,100*XBASE,120*XBASE,250*XBASE,700*XBASE],ATR(sclose,shigh,slow,1))

    def test_xatr(self):
        self.assertEqual([],XATR([],[],[]))
        shigh = [200,250,200,400]
        slow = [100,200,100,200]
        sclose = [150,220,150,300]
        self.assertEqual([666667,454545,679333,386667],XATR(sclose,shigh,slow))
        shigh.append(1000)
        slow.append(500)
        sclose.append(700)
        self.assertEqual([666667,454545,679333,386667,245171],XATR(sclose,shigh,slow))

    def test_strend(self):
        self.assertEqual([],STREND([]))
        self.assertEqual([0],STREND([1]))        
        source = [10,20,30,30,40,50,40,30,20,20,10,20]
        self.assertEqual([0,1,2,3,4,5,-1,-2,-3,-4,-5,1],STREND(source))
        source.append(20)
        self.assertEqual([0,1,2,3,4,5,-1,-2,-3,-4,-5,1,2],STREND(source))
        source.append(30)
        self.assertEqual([0,1,2,3,4,5,-1,-2,-3,-4,-5,1,2,3],STREND(source))
        source.append(20)
        self.assertEqual([0,1,2,3,4,5,-1,-2,-3,-4,-5,1,2,3,-1],STREND(source))
        source.append(10)
        self.assertEqual([0,1,2,3,4,5,-1,-2,-3,-4,-5,1,2,3,-1,-2],STREND(source))


    def test_tmax(self):
        self.assertEqual([],TMAX([],10))
        source = [10,12,3,2,5,100,0,13,16,9]
        self.assertEqual([10,12,3,2,5,100,0,13,16,9],TMAX(source,1))
        self.assertEqual([10,12,12,3,5,100,100,13,16,16],TMAX(source,2))
        source.append(3)
        source.append(30)
        self.assertEqual([10,12,12,3,5,100,100,13,16,16,9,30],TMAX(source,2))
        self.assertEqual([10,12,12,12,5,100,100,100,16,16,16,30],TMAX(source,3))

    def test_tmin(self):
        self.assertEqual([],TMIN([],10))
        source = [10,12,3,2,5,100,0,13,16,9]
        self.assertEqual([10,12,3,2,5,100,0,13,16,9],TMIN(source,1))
        self.assertEqual([10,10,3,2,2,5,0,0,13,9],TMIN(source,2))
        source.append(3)
        source.append(30)
        self.assertEqual([10,10,3,2,2,5,0,0,13,9,3,3],TMIN(source,2))
        self.assertEqual([10,10,3,2,2,2,0,0,0,9,3,3],TMIN(source,3))

    def test_nmax(self):
        self.assertEqual([],NMAX([]))
        source = [10,12,3,2,5,100,0,13,16,9]
        self.assertEqual([10,12,12,12,12,100,100,100,100,100],NMAX(source))
        source.append(3)
        source.append(103)
        self.assertEqual([10,12,12,12,12,100,100,100,100,100,100,103],NMAX(source))

    def test_nmin(self):
        self.assertEqual([],NMIN([]))
        source = [10,12,3,2,5,100,0,13,16,9]
        self.assertEqual([10,10,3,2,2,2,0,0,0,0],NMIN(source))
        source.append(3)
        source.append(-1)
        self.assertEqual([10,10,3,2,2,2,0,0,0,0,0,-1],NMIN(source))


    def test_cross(self):   #
        self.assertEqual([],UPCROSS([],[]))
        target = [10,20,30,40,50,40,30,20,10,12,11,12]
        follow = [5,15,35,41,60,50,25,26,8,12,13,12]
        self.assertEqual([0,0,1,0,0,0,0,1,0,0,1,0],UPCROSS(target,follow))
        self.assertEqual([1,0,0,0,0,0,1,0,1,0,0,0],DOWNCROSS(target,follow))
        target.append(15)
        follow.append(11)
        self.assertEqual([0,0,1,0,0,0,0,1,0,0,1,0,0],UPCROSS(target,follow))
        self.assertEqual([1,0,0,0,0,0,1,0,1,0,0,0,1],DOWNCROSS(target,follow))
        target.append(13)
        follow.append(25)
        self.assertEqual([0,0,1,0,0,0,0,1,0,0,1,0,0,1],UPCROSS(target,follow))
        self.assertEqual([1,0,0,0,0,0,1,0,1,0,0,0,1,0],DOWNCROSS(target,follow))

    def test_ncross(self):   #
        self.assertEqual([],NUPCROSS([],10))
        follow = [5,15,35,41,60,50,25,26,8,12,13,12]
        self.assertEqual([0,1,0,0,0,0,0,0,0,1,0,0],NUPCROSS(follow,10))
        self.assertEqual([1,0,0,0,0,0,0,0,1,0,0,0],NDOWNCROSS(follow,10))
        follow.append(8)
        self.assertEqual([0,1,0,0,0,0,0,0,0,1,0,0,0],NUPCROSS(follow,10))
        self.assertEqual([1,0,0,0,0,0,0,0,1,0,0,0,1],NDOWNCROSS(follow,10))
        follow.append(25)
        self.assertEqual([0,0,1,0,0,0,0,0,0,0,0,0,0,1],NUPCROSS(follow,15))
        self.assertEqual([1,0,0,0,0,0,0,0,1,0,0,0,1,0],NDOWNCROSS(follow,10))


    def test_ref(self):
        self.assertEqual([],REF([]))
        a= [1,2,3,4,5,6,7,8,9,0]
        self.assertEqual([1,2,3,4,5,6,7,8,9,0],REF(a,0))
        self.assertEqual([1,1,2,3,4,5,6,7,8,9],REF(a,1))
        self.assertEqual([1,1,1,2,3,4,5,6,7,8],REF(a,2))
        self.assertEqual([1,1,1,1,1,1,1,1,1,2],REF(a,8))
        self.assertEqual([1,1,1,1,1,1,1,1,1,1],REF(a,9))
        self.assertEqual([1,1,1,1,1,1,1,1,1,1],REF(a,10))
        self.assertEqual([1,1,1,1,1,1,1,1,1,1],REF(a,11))
        self.assertEqual([1,1,1,1,1,1,1,1,1,1],REF(a,100))
        a.append(100)
        self.assertEqual([1,1,1,2,3,4,5,6,7,8,9],REF(a,2))

    def test_minute(self):
        m1 = MINUTE([])
        self.assertEqual([],m1)
        ticks = [TICK(),TICK(),TICK(),TICK()]
        ticks[0].cname='IF1203'
        ticks[0].price = 100
        ticks[0].time = 91400000
        ticks[0].date = 20120111
        ticks[0].dvolume = 10
        ticks[0].holding = 10
        ticks[0].min1 = time2min(ticks[0].time)
        ticks[1].cname='IF1203'
        ticks[1].price = 110
        ticks[1].time = 91500000
        ticks[1].date = 20120111
        ticks[1].min1 = time2min(ticks[1].time)
        ticks[1].dvolume = 30
        ticks[1].holding = 11
        ticks[2].cname='IF1203'
        ticks[2].price = 115
        ticks[2].time = 91501000
        ticks[2].date = 20120111
        ticks[2].dvolume = 50
        ticks[2].holding = 12
        ticks[2].min1 = time2min(ticks[2].time)
        ticks[3].cname='IF1203'
        ticks[3].price = 91
        ticks[3].time = 91600000
        ticks[3].date = 20120111
        ticks[3].dvolume = 51
        ticks[3].holding = 13
        ticks[3].min1 = time2min(ticks[3].time)
        cm2 = MINUTE(ticks)
        m2 = cm2.smin1
        #print(m2)
        self.assertEqual(2,len(m2))
        self.assertEqual(100,m2[0].iclose)
        self.assertEqual(115,m2[1].iclose)
        self.assertEqual(100,m2[0].ilow)
        self.assertEqual(110,m2[1].ilow)
        self.assertEqual(100,m2[0].ihigh)
        self.assertEqual(115,m2[1].ihigh)
        self.assertEqual(914,m2[0].imin)
        self.assertEqual(915,m2[1].imin)
        self.assertEqual(0,m2[0].ivolume)
        self.assertEqual(40,m2[1].ivolume)
        self.assertEqual(base.ITYPE_UNKNOWN,m2[0].itype)
        self.assertEqual(base.ITYPE_L2H,m2[1].itype)
        
        self.assertTrue(cm2.modified)
        #
        ticks.extend([TICK(),TICK(),TICK()])
        ticks[4].cname='IF1203'
        ticks[4].price = 93
        ticks[4].time = 91601000
        ticks[4].date = 20120111
        ticks[4].min1 = time2min(ticks[4].time)
        ticks[4].dvolume = 80
        ticks[4].holding = 10
        ticks[5].cname='IF1203'
        ticks[5].price = 90
        ticks[5].time = 91602000
        ticks[5].date = 20120111
        ticks[5].dvolume = 88
        ticks[5].holding = 10
        ticks[5].min1 = time2min(ticks[5].time)
        ticks[6].cname='IF1203'
        ticks[6].price = 90
        ticks[6].time = 91700000
        ticks[6].date = 20120111
        ticks[6].dvolume = 89
        ticks[6].holding = 10
        ticks[6].min1 = time2min(ticks[6].time)
        cm2 = MINUTE(ticks)
        m2 = cm2.smin1
        self.assertEqual(90,m2[2].iclose)
        self.assertEqual(90,m2[2].ilow)
        self.assertEqual(93,m2[2].ihigh)
        self.assertEqual(916,m2[2].imin)
        self.assertEqual(38,m2[2].ivolume)
        self.assertEqual(base.ITYPE_H2L,m2[2].itype)
        self.assertTrue(cm2.modified)
        ##
        ticks.append(TICK())
        ticks[7].cname='IF1203'
        ticks[7].price = 91
        ticks[7].time = 91701000
        ticks[7].date = 20120111
        ticks[7].dvolume = 81
        ticks[7].holding = 10
        ticks[7].min1 = time2min(ticks[7].time)
        cm2 = MINUTE(ticks)
        m2 = cm2.smin1
        self.assertFalse(cm2.modified)
        self.assertEqual(916,m2[2].imin)
        ##测试终结符
        ticks.append(TICK())
        ticks[-1].cname='IF1203'
        ticks[-1].price = 0
        ticks[-1].time = 0
        ticks[-1].date = 0
        ticks[-1].dvolume = 0
        ticks[-1].holding = 0
        ticks[-1].min1 = time2min(ticks[-1].time)
        cm2 = MINUTE(ticks)
        m2 = cm2.smin1
        self.assertTrue(cm2.modified)
        self.assertEqual(917,m2[3].imin)
        ##测试重复的终结符
        ticks.append(TICK())
        ticks[-1].cname='IF1203'
        ticks[-1].price = 0
        ticks[-1].time = 0
        ticks[-1].date = 0
        ticks[-1].dvolume = 0
        ticks[-1].holding = 0
        ticks[-1].min1 = time2min(ticks[-1].time)
        cm2 = MINUTE(ticks)
        m2 = cm2.smin1
        self.assertFalse(cm2.modified)   #无变化
        self.assertEqual(917,m2[3].imin)
        ##测试pre_min
        spre_min1 = [BaseObject(),BaseObject()] #占位
        ticks = ticks[:4]
        cm2 = MINUTE(ticks,spre_min1=spre_min1)
        m2 = cm2.smin1
        self.assertEqual(4,len(m2))
        self.assertEqual(100,m2[2].iclose)
        self.assertEqual(115,m2[3].iclose)


if __name__ == "__main__":
    import logging
    logging.basicConfig(filename="test.log",level=logging.DEBUG,format='%(name)s:%(funcName)s:%(lineno)d:%(asctime)s %(levelname)s %(message)s')
    
    unittest.main()
